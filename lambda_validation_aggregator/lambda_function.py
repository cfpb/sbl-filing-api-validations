import os
import re
import json
import logging
import urllib.parse
import polars as pl
import boto3
import boto3.session
from botocore.exceptions import ClientError

from pydantic import PostgresDsn
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sbl_filing_api.entities.models.dao import SubmissionDAO, SubmissionState, FilingDAO
from regtech_data_validator.validator import get_scope_counts, ValidationPhase, ValidationResults, ValidationPhase
from regtech_data_validator.data_formatters import df_to_dicts, df_to_download
from regtech_data_validator.checks import Severity

log = logging.getLogger()
log.setLevel(logging.INFO)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    request = event['responsePayload'] if 'responsePayload' in event else event

    bucket = request['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(request['Records'][0]['s3']['object']['key'], encoding='utf-8')
    log.info(f"Received key: {key}")

    try:
        aggregate_validation_result(bucket, key)
    except Exception as e:
        log.exception('Failed to validate {} in {}'.format(key, bucket))
        raise e

def aggregate_validation_result(bucket, key):

    file_paths = [path for path in key.split('/') if path]
    file_name = file_paths[-1]
    period = file_paths[1]
    lei = file_paths[2]
    sub_id_regex = r"\d+"
    sub_match = re.match(sub_id_regex, file_name)
    sub_counter = int(sub_match.group())

    validation_report_path = f"{'/'.join(file_paths[:-1])}/{sub_counter}_report.csv"

    with get_db_session() as db_session:
        submission = (
            db_session.query(SubmissionDAO)
                .where(SubmissionDAO.filing == FilingDAO.id, FilingDAO.lei == lei, FilingDAO.filing_period == period, SubmissionDAO.counter == sub_counter)
        ).one()

        max_errors = os.getenv("MAX_ERRORS", 1000000)
        max_group_size = os.getenv("MAX_GROUP_SIZE", 200)

        if submission and submission.state not in [SubmissionState.SUBMISSION_ACCEPTED, SubmissionState.VALIDATION_EXPIRED, SubmissionState.SUBMISSION_UPLOAD_MALFORMED]:
            aws_session = boto3.session.Session()
            creds = aws_session.get_credentials()
            storage_options = {
                'aws_access_key_id': creds.access_key,
                'aws_secret_access_key': creds.secret_key,
                'session_token': creds.token,
                'aws_region': 'us-east-1',
            }
            lf = pl.scan_parquet(f"s3://{bucket}/{key}", allow_missing_columns=True, storage_options=storage_options)
            max_err_lf = lf.slice(0, max_errors + 1)
            df = max_err_lf.collect()
            error_counts, warning_counts = get_scope_counts(df)
            csv_df = pl.concat([df], how="diagonal")
            csv_content = df_to_download(csv_df, warning_counts.total_count, error_counts.total_count, max_errors)
            s3.put_object(Body=csv_content, Bucket=bucket, Key=validation_report_path)

            df = max_err_lf.group_by(pl.col("validation_id")).head(max_group_size).collect()

            validation_results = ValidationResults(
                error_counts=error_counts,
                warning_counts=warning_counts,
                is_valid=((error_counts.total_count + warning_counts.total_count) == 0),
                findings=df,
                phase=df.select(pl.first("phase")).item(),
            )

            final_df = pl.concat([df], how="diagonal")

            if final_df.is_empty():
                final_state = SubmissionState.VALIDATION_SUCCESSFUL
            else:
                final_state = (
                    SubmissionState.VALIDATION_WITH_ERRORS
                    if lf.filter(pl.col('validation_type') == 'Error').collect().height > 0
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
    secret = get_secret(os.getenv("DB_SECRET", None))
    postgres_dsn = PostgresDsn.build(
        scheme="postgresql+psycopg2",
        username=secret['username'],
        password=urllib.parse.quote(secret['password'], safe=""),
        host=secret['host'],
        path=secret['database'],
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
