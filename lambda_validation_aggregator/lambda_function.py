import os
import re
import json
import logging
import urllib.parse
import polars as pl
import boto3
import boto3.session
import asyncio
from botocore.exceptions import ClientError

from pydantic import PostgresDsn
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    async_scoped_session,
    AsyncSession
)
from sbl_filing_api.entities.models.dao import SubmissionDAO, SubmissionState, FilingDAO
from regtech_data_validator.validator import get_scope_counts, ValidationPhase, ValidationResults, ValidationPhase
from regtech_data_validator.data_formatters import df_to_dicts
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
        asyncio.run(aggregate_validation_result(bucket, key))
    except Exception as e:
        log.exception('Failed to validate {} in {}'.format(key, bucket))
        raise e

async def aggregate_validation_result(bucket, key):

    file_paths = [path for path in key.split('/') if path]
    file_name = file_paths[-1]
    period = file_paths[1]
    lei = file_paths[2]
    sub_id_regex = r"\d+"
    sub_match = re.match(sub_id_regex, file_name)
    sub_counter = sub_match.group()

    async with await get_db_session() as db_session:
        submission = await get_submission(db_session, lei, period, sub_counter)

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
            df = lf.collect()
            error_counts, warning_counts = get_scope_counts(df)

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
            
            final_df = build_validation_results(final_df, [validation_results], validation_results.phase)
            submission.state = final_state
            submission.validation_results = df
            await db_session.commit()

async def get_submission(session: AsyncSession, lei: str, period: str, counter: int):
    stmt = (
        select(SubmissionDAO)
        .where(SubmissionDAO.filing == FilingDAO.id, FilingDAO.lei == lei, FilingDAO.filing_period == period, SubmissionDAO.counter == counter)
    )
    return await session.scalar(stmt)

def build_validation_results(final_df: pl.DataFrame, results: list[ValidationResults], final_phase: ValidationPhase):
    max_records = 1000000
    max_group_size = 200
    val_json = df_to_dicts(final_df, max_records, max_group_size)
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

async def get_db_session() -> AsyncSession:
    secret = get_secret(os.getenv("DB_SECRET", None))
    postgres_dsn = PostgresDsn.build(
        scheme="postgresql+asyncpg",
        username=secret['username'],
        password=urllib.parse.quote(secret['password'], safe=""),
        host=secret['host'],
        path=secret['database'],
    )
    engine = create_async_engine(
        postgres_dsn.unicode_string(),
        echo=True,
    )
    SessionLocal = async_scoped_session(async_sessionmaker(engine, expire_on_commit=False), asyncio.current_task)
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


# if __name__ == '__main__':
#     lambda_handler(
#         {
#             "Records": [
#                 {
#                 "s3": {
#                     "bucket": {
#                         "name": "cfpb-regtech-devpub-lc-test"
#                     },
#                     "object": {
#                         "key": "upload/2024/1234364890REGTECH006/6254_pqs/"
#                     }
#                 }
#                 }
#             ]
#         }
#         , None)