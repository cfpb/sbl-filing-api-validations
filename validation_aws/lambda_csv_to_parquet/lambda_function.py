import json
import urllib.parse
import boto3
import logging

from sbl_validation_processor.csv_to_parquet import split_csv_into_parquet

log = logging.getLogger()
log.setLevel(logging.INFO)

def lambda_handler(event, context):
    log.info("Received event: " + json.dumps(event, indent=None))

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    log.info(f"Received key: {key}")
    if "report.csv" not in key:
        split_csv_into_parquet(bucket, key)
    else:
        raise RuntimeWarning("not processing report.csv: %s", key)
