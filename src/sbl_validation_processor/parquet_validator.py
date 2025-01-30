from typing import List
import boto3
import boto3.session
import os
import json
import logging
import polars as pl
import re
import urllib.parse

from io import BytesIO
from botocore.exceptions import ClientError
from pydantic import PostgresDsn
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from regtech_data_validator.validator import validate_lazy_frame
from regtech_data_validator.validation_results import ValidationResults, ValidationPhase

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO)


def scan_parquets(bucket: str, key: str):
    env = os.getenv("ENV", "S3")
    if env == "LOCAL":
        return pl.scan_parquet(os.path.join(bucket, key), allow_missing_columns=True)
    else:
        session = boto3.session.Session()
        creds = session.get_credentials()
        storage_options = {
            "aws_access_key_id": creds.access_key,
            "aws_secret_access_key": creds.secret_key,
            "session_token": creds.token,
            "aws_region": "us-east-1",
        }
        return pl.scan_parquet(
            f"s3://{bucket}/{key}",
            allow_missing_columns=True,
            storage_options=storage_options,
        )


def write_parquet(buffer: BytesIO, bucket: str, parquet_file: str):
    env = os.getenv("ENV", "S3")
    if env == "LOCAL":
        file_path = os.path.join(bucket, parquet_file)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(buffer.getvalue())
    else:
        s3 = boto3.client("s3")
        s3.upload_fileobj(buffer, bucket, parquet_file)


def validate_parquets(bucket: str, key: str):
    log.info(f"Validating parquets in {bucket}, File {key}")

    file_paths = [path for path in key.split("/") if path]
    file_name = file_paths[-1]
    lei = file_paths[-2]
    sub_id_regex = r"\d+"
    sub_match = re.match(sub_id_regex, file_name)
    submission_id = sub_match.group()

    pq_idx = 1
    batch_size = int(os.getenv("BATCH_SIZE", 50000))
    max_errors = int(os.getenv("MAX_ERRORS", 1000000))
    persist_db = bool(json.loads(os.getenv("DB_PERSIST", "false").lower()))

    if root := os.getenv("S3_ROOT"):
        validation_result_path = (
            f"{root}/{'/'.join(file_paths[1:-1])}/{submission_id}_res/"
        )
    else:
        validation_result_path = f"{'/'.join(file_paths[:-1])}/{submission_id}_res/"

    try:
        lf = scan_parquets(bucket, key)
        all_results = []

        for validation_results in validate_lazy_frame(
            lf, {"lei": lei}, batch_size=batch_size, max_errors=max_errors
        ):
            if validation_results.findings.height:
                buffer = BytesIO()
                df = validation_results.findings.with_columns(
                    phase=pl.lit(validation_results.phase),
                    submission_id=pl.lit(submission_id),
                )
                df = df.cast({"phase": pl.String})
                log.info("findings found for batch {}: {}".format(pq_idx, df.height))
                if persist_db:
                    db_session = get_db_session()
                    db_entries = df.write_database(
                        table_name="findings",
                        connection=db_session,
                        if_table_exists="append",
                    )
                    db_session.commit()
                    log.info("{} findings persisted to db".format(db_entries))
                df.write_parquet(buffer)
                buffer.seek(0)
                write_parquet(
                    buffer, bucket, f"{validation_result_path}{pq_idx:05}.parquet"
                )
                pq_idx += 1
            validation_results.findings = None
            all_results.append(validation_results)

        validation_results = combine_results(all_results)

        return {
            "statusCode": 200,
            "body": json.dumps("done validating!"),
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": bucket},
                        "object": {"key": validation_result_path},
                    },
                    "results": validation_results,
                }
            ],
        }
    except Exception as e:
        log.exception("Failed to validate {} in {}".format(key, bucket))
        raise e


def combine_results(results: List[ValidationResults]):
    if any(
        [
            True
            for r in results
            if r.phase == ValidationPhase.SYNTACTICAL and not r.is_valid
        ],
    ):
        syntax_error_counts = sum([r.error_counts.single_field_count for r in results])
        val_res = {
            "total_records": sum([r.record_count for r in results]),
            "syntax_errors": {
                "single_field_count": syntax_error_counts,
                "multi_field_count": 0,  # this will always be zero for syntax errors
                "register_count": 0,  # this will always be zero for syntax errors
                "total_count": syntax_error_counts,
            },
        }
    else:
        val_res = {
            "total_records": sum(
                [r.record_count for r in results if r.phase == ValidationPhase.LOGICAL]
            ),
            "syntax_errors": {
                "single_field_count": 0,
                "multi_field_count": 0,
                "register_count": 0,
                "total_count": 0,
            },
            "logic_errors": {
                "single_field_count": sum(
                    [r.error_counts.single_field_count for r in results]
                ),
                "multi_field_count": sum(
                    [r.error_counts.multi_field_count for r in results]
                ),
                "register_count": sum([r.error_counts.register_count for r in results]),
                "total_count": sum([r.error_counts.total_count for r in results]),
            },
            "logic_warnings": {
                "single_field_count": sum(
                    [r.warning_counts.single_field_count for r in results]
                ),
                "multi_field_count": sum(
                    [r.warning_counts.multi_field_count for r in results]
                ),
                "register_count": sum(
                    [r.warning_counts.register_count for r in results]
                ),
                "total_count": sum([r.warning_counts.total_count for r in results]),
            },
        }
    return val_res


def get_db_session():
    SessionLocal = sessionmaker(bind=get_filing_engine())
    session = SessionLocal()
    return session


def get_filing_engine():
    env = os.getenv("ENV", "S3")
    if env == "LOCAL":
        user = os.getenv("DB_USER")
        passwd = os.getenv("DB_PWD")
        host = os.getenv("DB_HOST")
        db = os.getenv("DB_NAME")
    else:
        secret = get_secret(os.getenv("DB_SECRET", None))
        user = secret["username"]
        passwd = secret["password"]
        host = secret["host"]
        db = secret["database"]

    postgres_dsn = PostgresDsn.build(
        scheme="postgresql+psycopg2",
        username=user,
        password=urllib.parse.quote(passwd, safe=""),
        host=host,
        path=db,
    )
    conn_str = str(postgres_dsn)
    return create_engine(conn_str)


def get_secret(secret_name):
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)
