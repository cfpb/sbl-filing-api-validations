import json
import urllib.parse
import boto3
import logging
import os

from sbl_validation_processor.csv_to_parquet import split_csv_into_parquet

log = logging.getLogger()
log.setLevel(logging.INFO)

eb = boto3.client("events")


def lambda_handler(event, context):
    log.info("Received event: " + json.dumps(event, indent=None))

    if "detail" in event:
        request = event["detail"]
    else:
        request = event["Records"][0]["s3"]

    bucket = request["bucket"]["name"]
    key = urllib.parse.unquote_plus(request["object"]["key"], encoding="utf-8")
    log.info(f"Received key: {key}")
    if "report.csv" not in key:
        eb_response = eb.put_events(
            Entries=[
                {
                    "Detail": json.dumps(split_csv_into_parquet(bucket, key)),
                    "DetailType": "csv_to_parquet",
                    "EventBusName": os.getenv("EVENT_BUS", "default"),
                    "Source": "csv_to_parquet",
                }
            ]
        )
        log.info("put event done")
        log.info(eb_response)
    else:
        raise RuntimeWarning("not processing report.csv: %s", key)
