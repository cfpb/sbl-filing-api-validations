import logging
import os
import urllib.parse
import boto3
import json

from sbl_validation_processor.parquet_validator import validate_parquets

log = logging.getLogger()
log.setLevel(logging.INFO)
eb = boto3.client("events")


def lambda_handler(event, context):
    if "detail" in event:
        request = event["detail"]
    elif "responsePayload" in event:
        request = event["responsePayload"]
    else:
        request = event

    bucket = request["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        request["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    log.info(f"Received key: {key}")

    eb_response = eb.put_events(
        Entries=[
            {
                "Detail": json.dumps(validate_parquets(bucket, key)),
                "DetailType": "parquet_validator",
                "EventBusName": os.getenv("EVENT_BUS", "default"),
                "Source": "parquet_validator",
            }
        ]
    )
    log.info("put event done")
    log.info(eb_response)
