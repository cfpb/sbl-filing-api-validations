import os
import re
import json
import logging
import urllib.parse
import polars as pl
import boto3
import boto3.session
from botocore.exceptions import ClientError

from io import BytesIO
from pydantic import PostgresDsn
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sbl_filing_api.entities.models.dao import SubmissionDAO, SubmissionState, FilingDAO
from regtech_data_validator.validator import get_scope_counts, ValidationPhase, ValidationResults
from regtech_data_validator.data_formatters import df_to_dicts, df_to_download
from regtech_data_validator.checks import Severity

log = logging.getLogger()

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

def aggregate_validation_results(bucket, key):
    file_paths = [path for path in key.split('/') if path]
    file_name = file_paths[-1]
    period = file_paths[-3]
    lei = file_paths[-2]
    sub_id_regex = r"\d+"
    sub_match = re.match(sub_id_regex, file_name)
    sub_counter = int(sub_match.group())

    if root := os.getenv('S3_ROOT'):
        validation_report_path = f"{root}/{'/'.join(file_paths[1:-1])}/{sub_counter}_report.csv"
    else:
        validation_report_path = f"{'/'.join(file_paths[:-1])}/{sub_counter}_report.csv"

    with get_db_session() as db_session:
        submission = (
            db_session.query(SubmissionDAO)
                .where(SubmissionDAO.filing == FilingDAO.id, FilingDAO.lei == lei, FilingDAO.filing_period == period, SubmissionDAO.counter == sub_counter)
        ).one()

        max_errors = os.getenv("MAX_ERRORS", 1000000)
        max_group_size = os.getenv("MAX_GROUP_SIZE", 200)

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
            df = lf.collect()
            error_counts, warning_counts = get_scope_counts(df)
            #slice is start indice inclusive, so 0 to max_errors will return 1000000 errors (0-999999) if the 
            #max_errors is 1000000 and there are more than that.  Adding +1 actually returns 
            #max_errors + 1 which would be one more than the max_errors intended
            final_df = df.slice(0, max_errors)
            
            #build report csv and push to S3
            csv_content = df_to_download(final_df, warning_counts.total_count, error_counts.total_count, max_errors)
            write_report(csv_content, bucket, validation_report_path)

            #truncate the final_df again for the json validation results we send to the frontend
            if not final_df.is_empty():
                final_df = lf.group_by(pl.col("validation_id")).head(max_group_size).collect()

            validation_results = ValidationResults(
                error_counts=error_counts,
                warning_counts=warning_counts,
                is_valid=((error_counts.total_count + warning_counts.total_count) == 0),
                findings=final_df,
                phase=final_df.select(pl.first("phase")).item() if not final_df.is_empty() else ValidationPhase.LOGICAL,
            )

            if validation_results.is_valid:
                final_state = SubmissionState.VALIDATION_SUCCESSFUL
            else:
                final_state = (
                    SubmissionState.VALIDATION_WITH_ERRORS
                    if validation_results.error_counts.total_count != 0
                    else SubmissionState.VALIDATION_WITH_WARNINGS
                )
            
            validation_res = build_validation_results(final_df, [validation_results], validation_results.phase)
            submission.state = final_state
            submission.validation_results = validation_res
            db_session.commit()

def build_validation_results(final_df: pl.DataFrame, results: list[ValidationResults], final_phase: ValidationPhase):
    val_json = df_to_dicts(final_df, int(os.getenv("MAX_RECORDS", 1000000)), int(os.getenv("MAX_GROUP_SIZE", 200)))
    if final_phase == ValidationPhase.SYNTACTICAL:
        syntax_error_counts = sum([r.error_counts.single_field_count for r in results])
        val_res = {
            "syntax_errors": {
                "single_field_count": syntax_error_counts,
                "multi_field_count": 0,  # this will always be zero for syntax errors
                "register_count": 0,  # this will always be zero for syntax errors
                "total_count": syntax_error_counts,
                "details": val_json,
            }
        }
    else:
        errors_list = [e for e in val_json if e["validation"]["severity"] == Severity.ERROR]
        warnings_list = [w for w in val_json if w["validation"]["severity"] == Severity.WARNING]
        val_res = {
            "syntax_errors": {
                "single_field_count": 0,
                "multi_field_count": 0,
                "register_count": 0,
                "total_count": 0,
                "details": [],
            },
            "logic_errors": {
                "single_field_count": sum([r.error_counts.single_field_count for r in results]),
                "multi_field_count": sum([r.error_counts.multi_field_count for r in results]),
                "register_count": sum([r.error_counts.register_count for r in results]),
                "total_count": sum([r.error_counts.total_count for r in results]),
                "details": errors_list,
            },
            "logic_warnings": {
                "single_field_count": sum([r.warning_counts.single_field_count for r in results]),
                "multi_field_count": sum([r.warning_counts.multi_field_count for r in results]),
                "register_count": sum([r.warning_counts.register_count for r in results]),
                "total_count": sum([r.warning_counts.total_count for r in results]),
                "details": warnings_list,
            },
        }

    return val_res

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