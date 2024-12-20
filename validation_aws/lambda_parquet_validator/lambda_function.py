import logging
import re
import urllib.parse

from sbl_validation_processor.csv_to_parquet import validate_parquets

log = logging.getLogger()

def lambda_handler(event, context):
    request = event['responsePayload'] if 'responsePayload' in event else event

    bucket = request['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(request['Records'][0]['s3']['object']['key'], encoding='utf-8')
    log.info(f"Received key: {key}")

    validate_parquets(bucket, key)