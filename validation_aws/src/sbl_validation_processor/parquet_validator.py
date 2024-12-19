import boto3
import boto3.session
import os
import io
import json
import logging
import polars as pl
import re


from botocore.exceptions import ClientError
from pydantic import PostgresDsn
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker, Session

from regtech_data_validator.validator import validate_lazy_frame

log = logging.getLogger()

def validate_parquets(bucket: str, key: str):
    print(f"Validating parquets in {bucket}, File {key}", flush=True)
    s3 = boto3.client('s3')

    file_paths = [path for path in key.split('/') if path]
    file_name = file_paths[-1]
    lei = file_paths[2]
    sub_id_regex = r"\d+"
    sub_match = re.match(sub_id_regex, file_name)
    submission_id = sub_match.group()

    pq_idx = 1
    batch_size = int(os.getenv("BATCH_SIZE", 50000))
    max_errors = int(os.getenv("MAX_ERRORS", 1000000))
    persist_db = bool(json.loads(os.getenv("DB_PERSIST", "false").lower()))
    log.info(f"batch size: {batch_size}")

    session = boto3.session.Session()
    creds = session.get_credentials()
    storage_options = {
        'aws_access_key_id': creds.access_key,
        'aws_secret_access_key': creds.secret_key,
        'session_token': creds.token,
        'aws_region': 'us-east-1',
    }

    validation_result_path = f"{'/'.join(file_paths[:-1])}/{submission_id}_res/"
    print(f"Validating result path {validation_result_path}", flush=True)

    try:
        lf = pl.scan_parquet(f"s3://{bucket}/{key}", allow_missing_columns=True, storage_options=storage_options)

        for validation_results in validate_lazy_frame(lf, {"lei": lei}, batch_size=batch_size, max_errors=max_errors):
            if validation_results.findings.height:
                buffer = io.BytesIO()
                df = validation_results.findings.with_columns(phase=pl.lit(validation_results.phase), submission_id=pl.lit(submission_id))
                df = df.cast({"phase": pl.String})
                log.info("findings found for batch {}: {}".format(pq_idx, df.height))
                if persist_db:
                    db_session = get_db_session()
                    db_entries = df.write_database(table_name="findings", connection=db_session, if_table_exists="append")
                    db_session.commit()
                    log.info("{} findings persisted to db".format(db_entries))
                df.write_parquet(buffer)
                buffer.seek(0)
                s3.upload_fileobj(buffer, bucket, f"{validation_result_path}{pq_idx:05}.parquet")
                pq_idx += 1

        return {
            'statusCode': 200,
            'body': json.dumps('done validating!'),
            "Records": [
                {
                    "s3": {
                        "bucket": {
                            "name": bucket
                        },
                        "object": {
                            "key": validation_result_path
                        }
                    }
                }
            ]
        }
    except Exception as e:
        log.exception('Failed to validate {} in {}'.format(key, bucket))
        raise e
    
def get_db_session():
    SessionLocal = sessionmaker(bind=get_filing_engine())
    session = SessionLocal()
    return session

def get_filing_engine():
    secret = get_secret(os.getenv("DB_SECRET", None))
    postgres_dsn = PostgresDsn.build(
        scheme="postgresql+psycopg2",
        username=secret['username'],
        password=urllib.parse.quote(secret['password'], safe=""),
        host=secret['host'],
        path=secret['database'],
    )
    conn_str = str(postgres_dsn)
    return create_engine(conn_str)

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
