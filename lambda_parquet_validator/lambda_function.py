import os
import re
import io
import logging
import urllib.parse
import polars as pl
import boto3
import boto3.session

from regtech_data_validator.validator import validate_lazy_frame

log = logging.getLogger()
log.setLevel(logging.INFO)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    request = event['responsePayload'] if 'responsePayload' in event else event

    bucket = request['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(request['Records'][0]['s3']['object']['key'], encoding='utf-8')
    log.info(f"Received key: {key}")

    file_paths = [path for path in key.split('/') if path]
    file_name = file_paths[-1]
    lei = file_paths[2]
    sub_id_regex = r"\d+"
    sub_match = re.match(sub_id_regex, file_name)
    submission_id = sub_match.group()

    pq_idx = 1
    batch_size = int(os.getenv("BATCH_SIZE", 50000))
    max_errors = int(os.getenv("MAX_ERRORS", 1000000))
    log.info(f"batch size: {batch_size}")

    session = boto3.session.Session()
    creds = session.get_credentials()
    storage_options = {
        'aws_access_key_id': creds.access_key,
        'aws_secret_access_key': creds.secret_key,
        'session_token': creds.token,
        'aws_region': 'us-east-1',
    }

    try:
        lf = pl.scan_parquet(f"s3://{bucket}/{key}", storage_options=storage_options).fill_null('')

        for validation_results in validate_lazy_frame(lf, {"lei": lei}, batch_size=batch_size, max_errors=max_errors):
            buffer = io.BytesIO()
            df = validation_results.findings.with_columns(phase=pl.lit(validation_results.phase), submission_id=pl.lit(submission_id))
            df.write_parquet(buffer)
            buffer.seek(0)
            s3.upload_fileobj(buffer, bucket, f"{'/'.join(file_paths[:-1])}/{submission_id}_res/{pq_idx}.parquet")
            pq_idx += 1
    except Exception as e:
        log.exception('Failed to validate {} in {}'.format(key, bucket))
        raise e