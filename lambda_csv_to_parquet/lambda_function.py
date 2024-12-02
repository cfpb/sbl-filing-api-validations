import json
import urllib.parse
import boto3
import os
import logging

import pandas as pa
import io

log = logging.getLogger()
log.setLevel(logging.INFO)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    log.info("Received event: " + json.dumps(event, indent=None))

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    log.info(f"Received key: {key}")
    paths = key.split('/')
    fname = paths[-1]
    fprefix = '.'.join(fname.split('.')[:-1])
    res_folder = f"{'/'.join(paths[:-1])}/{fprefix}_pqs/"
    try:
        pq_idx = 1
        response = s3.get_object(Bucket=bucket, Key=key)
        batch_size = int(os.getenv('BATCH_SIZE', 50000))
        log.info(f"batch size: {batch_size}")
        
        for chunk in pa.read_csv(response['Body'], dtype=str, keep_default_na=False, chunksize=batch_size):
            buffer = io.BytesIO()
            chunk.to_parquet(buffer)
            buffer.seek(0)
            s3.upload_fileobj(buffer, bucket, f"{res_folder}{pq_idx:05}.parquet")
            pq_idx += 1

        return {
            'statusCode': 200,
            'body': json.dumps('done converting!'),
            "Records": [
                {
                    "s3": {
                        "bucket": {
                            "name": bucket
                        },
                        "object": {
                            "key": res_folder
                        }
                    }
                }
            ]
        }
    except Exception as e:
        log.exception('Failed to process {} in {}'.format(key, bucket))
        raise e