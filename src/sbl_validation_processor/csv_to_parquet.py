import boto3
import json
import logging
import os
import pandas as pa

from io import BytesIO

log = logging.getLogger()


def get_csv_data(bucket: str, key: str):
    env = os.getenv("ENV", "S3")
    if env == "LOCAL":
        with open(os.path.join(bucket, key), "r") as f:
            return pa.io.common.StringIO(f.read())
    else:
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)
        return response["Body"]


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


def split_csv_into_parquet(bucket: str, key: str):
    try:
        paths = key.split("/")
        fname = paths[-1]
        fprefix = ".".join(fname.split(".")[:-1])
        if root := os.getenv("S3_ROOT"):
            res_folder = f"{root}/{'/'.join(paths[1:-1])}/{fprefix}_pqs/"
        else:
            res_folder = f"{'/'.join(paths[:-1])}/{fprefix}_pqs/"

        csv_data = get_csv_data(bucket, key)

        pq_idx = 1
        batch_size = int(os.getenv("BATCH_SIZE", 50000))
        log.info(f"batch size: {batch_size}")
        for chunk in pa.read_csv(
            csv_data, dtype=str, keep_default_na=False, chunksize=batch_size
        ):
            buffer = BytesIO()
            chunk.to_parquet(buffer)
            buffer.seek(0)
            write_parquet(buffer, bucket, f"{res_folder}{pq_idx:05}.parquet")
            pq_idx += 1

        return {
            "statusCode": 200,
            "body": json.dumps("done converting!"),
            "Records": [
                {"s3": {"bucket": {"name": bucket}, "object": {"key": res_folder}}}
            ],
        }
    except Exception as e:
        log.exception("Failed to process {} in {}".format(key, bucket))
        raise e
