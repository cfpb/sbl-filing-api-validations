import argparse
import os
import boto3
import json
import logging

from sbl_validation_processor.parquet_validator import validate_parquets

logger = logging.getLogger()

def do_validation(bucket: str, key: str):
    validate_parquets(bucket, key)

    paths = [p for p in key.split("/") if p]
    sub_id = paths[-1].split("_")[0]

    s3 = boto3.client("s3")
    r = s3.put_object(
        Bucket=bucket,
        Key="/".join(paths[:-1]) + f"/{sub_id}.done_res",
        Body=f"{sub_id} parquet validation done".encode("utf-8"),
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Parquet Validator Job")
    parser.add_argument("--bucket")
    parser.add_argument("--key")
    args = parser.parse_args()
    print(args)
    if not args.bucket or not args.key:
        logger.error("Error running parquet validator job.  --bucket and --key must be present.")
    else:
        do_validation(args.bucket, args.key)