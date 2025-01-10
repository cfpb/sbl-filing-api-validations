import os
import re
import json
import logging
from typing import Dict, List
import urllib.parse
from warnings import deprecated
import polars as pl
import boto3
import boto3.session
import gc
from botocore.exceptions import ClientError

from io import BytesIO
import psutil
from pydantic import PostgresDsn
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sbl_filing_api.entities.models.dao import SubmissionDAO, SubmissionState, FilingDAO
from regtech_data_validator.validator import get_scope_counts, ValidationPhase, ValidationResults
from regtech_data_validator.data_formatters import df_to_dicts, df_to_download
from regtech_data_validator.checks import Severity

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO)

def get_parquet_paths(bucket: str, key: str):
    env = os.getenv('ENV', 'S3')
    if env == 'LOCAL':
        dir_path = os.path.join(bucket, key)
        if not os.path.isdir(dir_path):
            return [], {}
        return [
            os.path.join(dir_path, file) for file in os.listdir(dir_path) if file.endswith(".parquet")
        ], {}
    else:
        aws_session = boto3.session.Session()
        creds = aws_session.get_credentials()
        storage_options = {
            'aws_access_key_id': creds.access_key,
            'aws_secret_access_key': creds.secret_key,
            'session_token': creds.token,
            'aws_region': 'us-east-1',
        }

        s3 = boto3.client('s3')
        s3_objs = s3.list_objects_v2(Bucket=bucket, Prefix=key)
        return [f"s3://{bucket}/{obj['Key']}" for obj in s3_objs.get('Contents', []) if obj['Key'].endswith(".parquet")], storage_options

def write_report(report_data: BytesIO, bucket: str, report_file: str):
    env = os.getenv('ENV', 'S3')
    if env == 'LOCAL':
        file_path = os.path.join(bucket, report_file)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(report_data)
    else:
        s3 = boto3.client('s3')
        s3.put_object(Body=report_data, Bucket=bucket, Key=report_file)
        log.info("completed report upload")

        # part_size = int(os.getenv('UPLOAD_PART_SIZE', str(1024 * 1024 * 50)))
        # multipart = s3.create_multipart_upload(
        #     Bucket=bucket,
        #     Key=report_file,
        # )

        # upload_id = multipart["UploadId"]

        # log.info(f"upload_id: {upload_id}")
        # part = 1

        # parts = []

        # while bytes := report_data.read(part_size):
        #     response = s3.upload_part(
        #         Body=bytes,
        #         Bucket=bucket,
        #         Key=report_file,
        #         PartNumber=part,
        #         UploadId=upload_id,
        #     )

        #     del bytes
        #     gc.collect()

        #     parts.append({
        #         'ETag': response['ETag'],
        #         'PartNumber': part
        #     })

        #     log.info(f"finished uploading part: {part}")

        #     part += 1

        # response = s3.complete_multipart_upload(
        #     Bucket=bucket,
        #     Key=report_file,
        #     MultipartUpload={
        #         'Parts': parts,
        #     },
        #     UploadId=upload_id,
        # )
        # log.info("completed multi-part upload")
        # log.info(response)


def aggregate_validation_results(bucket, key, results):
    file_paths = [path for path in key.split('/') if path]
    file_name = file_paths[-1]
    period = file_paths[-3]
    lei = file_paths[-2]
    sub_id_regex = r"\d+"
    sub_match = re.match(sub_id_regex, file_name)
    sub_counter = int(sub_match.group())

    process = psutil.Process()
    mb_factor = 1024 ** 2

    if root := os.getenv('S3_ROOT'):
        validation_report_path = f"{root}/{'/'.join(file_paths[1:-1])}/{sub_counter}_report.csv"
    else:
        validation_report_path = f"{'/'.join(file_paths[:-1])}/{sub_counter}_report.csv"

    with get_db_session() as db_session:
        submission = (
            db_session.query(SubmissionDAO)
                .where(SubmissionDAO.filing == FilingDAO.id, FilingDAO.lei == lei, FilingDAO.filing_period == period, SubmissionDAO.counter == sub_counter)
        ).one()

        max_errors = int(os.getenv("MAX_ERRORS", 1000000))
        max_group_size = int(os.getenv("MAX_GROUP_SIZE", 200))

        if submission and submission.state not in [SubmissionState.SUBMISSION_ACCEPTED, SubmissionState.VALIDATION_EXPIRED, SubmissionState.SUBMISSION_UPLOAD_MALFORMED]:

            file_paths, storage_options = get_parquet_paths(bucket, key)

            #scan each result parquet into a lazyframe then diagonally concat so all columns are merged into the final lf.  Otherwise
            #this will error if trying to scan a parquet directory and the parquets don't contain the same columns (particularly the
            #field/value columns)
            lazyframes = [pl.scan_parquet(file, allow_missing_columns=True, storage_options=storage_options) for file in file_paths]
            lf = pl.LazyFrame()
            if lazyframes:
                lf = pl.concat(lazyframes, how="diagonal")
            
            #get the real total count of errors and warnings before truncating based on max error length
            error_counts, warning_counts = get_error_and_warning_totals(results)
            #slice is start indice inclusive, so 0 to max_errors will return 1000000 errors (0-999999) if the 
            #max_errors is 1000000 and there are more than that.  Adding +1 actually returns 
            #max_errors + 1 which would be one more than the max_errors intended
            log.info(f"mem before lf collect: {process.memory_info().rss / mb_factor} MB")
            max_err_lf = lf.slice(0, max_errors)
            final_df = max_err_lf.collect()
            log.info(f"mem after lf collect: {process.memory_info().rss / mb_factor} MB")
            
            #build report csv and push to S3

            force_gc = bool(json.loads(os.getenv("FORCE_GC", "false").lower()))

            if force_gc:
                print(f"test gc collect: {gc.collect()}")
            
            csv_content = df_to_download(final_df, warning_counts, error_counts, max_errors)
            log.info("got content buffer ok")
            write_report(csv_content, bucket, validation_report_path)

            if force_gc:
                del csv_content
                print(f"test gc collect 2: {gc.collect()}")

            #truncate the final_df again for the json validation results we send to the frontend
            if not final_df.is_empty():
                use_max_err_lf = bool(json.loads(os.getenv("USE_MAX_ERR_LF", "false").lower()))

                log.info(f"mem before json lf collect with using truncated lf? {use_max_err_lf}: {process.memory_info().rss / mb_factor} MB")

                lf_to_use = max_err_lf if use_max_err_lf else lf

                validation_groups = lf_to_use.select("validation_id").unique().collect()

                log.info(f"total validation groups: {validation_groups.height}")

                validation_group_results = []

                for validation_id in validation_groups["validation_id"]:
                    log.info(f"valiation_id: {validation_id}")
                    
                    validation_group_result = lf.filter(pl.col("validation_id") == validation_id).head(max_group_size).collect()
                    log.info(f"validation_results: {validation_group_result.height}")

                    validation_group_results.extend(df_to_dicts(validation_group_result))
                    log.info(f"validation groups iter mem: {process.memory_info().rss / mb_factor} MB")

                log.info(f"mem after json lf collect with using truncated lf? {use_max_err_lf}: {process.memory_info().rss / mb_factor} MB")

            if error_counts + warning_counts == 0:
                final_state = SubmissionState.VALIDATION_SUCCESSFUL
            else:
                final_state = (
                    SubmissionState.VALIDATION_WITH_ERRORS
                    if error_counts != 0
                    else SubmissionState.VALIDATION_WITH_WARNINGS
                )
            
            build_final_json(validation_group_results, results)
            submission.state = final_state
            submission.validation_results = results
            db_session.commit()

def get_error_and_warning_totals(results):
    if results["syntax_errors"]["total_count"] > 0:
        return results["syntax_errors"]["total_count"], 0 #syntax are only error counts
    else:
        return results["logic_errors"]["total_count"], results["logic_warnings"]["total_count"]

def build_final_json(val_json: List[Dict], results: Dict):
    if results["syntax_errors"]["total_count"] > 0:
        results["syntax_errors"]["details"] = val_json
    else:
        errors_list = [e for e in val_json if e["validation"]["severity"] == Severity.ERROR]
        warnings_list = [w for w in val_json if w["validation"]["severity"] == Severity.WARNING]
        results["syntax_errors"]["details"] = []
        results["logic_errors"]["details"] = errors_list
        results["logic_warnings"]["details"] = warnings_list


@deprecated
def build_validation_results(final_df: pl.DataFrame, results: dict):
    val_json = df_to_dicts(final_df, max_group_size=int(os.getenv("MAX_GROUP_SIZE", 200)))
    if results["syntax_errors"]["total_count"] > 0:
        results["syntax_errors"]["details"] = val_json
    else:
        errors_list = [e for e in val_json if e["validation"]["severity"] == Severity.ERROR]
        warnings_list = [w for w in val_json if w["validation"]["severity"] == Severity.WARNING]
        results["syntax_errors"]["details"] = []
        results["logic_errors"]["details"] = errors_list
        results["logic_warnings"]["details"] = warnings_list

def get_db_session() -> Session:
    env = os.getenv('ENV', 'S3')
    if env == 'LOCAL':
        user=os.getenv("DB_USER")
        passwd=os.getenv("DB_PWD")
        host=os.getenv("DB_HOST")
        db=os.getenv("DB_NAME")
    else:
        secret = get_secret(os.getenv("DB_SECRET", None))
        user=secret['username']
        passwd=secret['password']
        host=secret['host']
        db=secret['database']

    postgres_dsn = PostgresDsn.build(
        scheme="postgresql+psycopg2",
        username=user,
        password=urllib.parse.quote(passwd, safe=""),
        host=host,
        path=db,
    )
    engine = create_engine(
        postgres_dsn.unicode_string(),
        echo=True,
    )
    SessionLocal = scoped_session(sessionmaker(engine, expire_on_commit=False))
    return SessionLocal()

def get_secret(secret_name):
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)