import argparse
import os
import boto3
import json
import logging

from sbl_validation_processor.parquet_validator import validate_parquets

logger = logging.getLogger()


def fire_validation_done(response):
    eb = boto3.client("events")
    eb.put_events(
        Entries=[
            {
                "Detail": json.dumps(response),
                "DetailType": "parquet_validator",
                "EventBusName": os.getenv("EVENT_BUS", "default"),
                "Source": "parquet_validator",
            }
        ]
    )


def do_validation(bucket: str, key: str):
    validation_response = validate_parquets(bucket, key)

    fire_validation_done(validation_response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parquet Validator Job")
    parser.add_argument("--bucket")
    parser.add_argument("--key")
    args = parser.parse_args()
    if not args.bucket or not args.key:
        logger.error(
            "Error running parquet validator job.  --bucket and --key must be present."
        )
    else:
        do_validation(args.bucket, args.key)
