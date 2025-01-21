import argparse
import os
import boto3
import json
import logging

from sbl_validation_processor.csv_to_parquet import split_csv_into_parquet

logger = logging.getLogger()


def fire_parquet_done(response):
    eb = boto3.client("events")
    eb.put_events(
        Entries=[
            {
                "Detail": json.dumps(response),
                "DetailType": "csv_to_parquet",
                "EventBusName": os.getenv("EVENT_BUS", "default"),
                "Source": "csv_to_parquet",
            }
        ]
    )


def do_validation(bucket: str, key: str):
    response = split_csv_into_parquet(bucket, key)

    fire_parquet_done(response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parquet Splitter Job")
    parser.add_argument("--bucket")
    parser.add_argument("--key")
    args = parser.parse_args()
    if not args.bucket or not args.key:
        logger.error(
            "Error running parquet splitter job.  --bucket and --key must be present."
        )
    else:
        do_validation(args.bucket, args.key)
