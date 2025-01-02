import os
import boto3
import json
import logging

from sbl_validation_processor.csv_to_parquet import split_csv_into_parquet

logger = logging.getLogger()
logger.setLevel("INFO")

def fire_parquet_done(response):
    key = response["Records"][0]["s3"]["object"]["key"]
    bucket = response["Records"][0]["s3"]["bucket"]["name"]
    eb = boto3.client('events')
    eb_response = eb.put_events(
        Entries=[
            {
                'Detail': json.dumps(response),
                'DetailType': 'csv_to_parquet',
                'EventBusName': os.getenv('EVENT_BUS', 'default'),
                'Source': 'csv_to_parquet',
            }
        ]
    )

def watch_queue():
    region_name = "us-east-1"

    session = boto3.session.Session()
    sqs = session.client(service_name='sqs', region_name=region_name)

    while True:
        response = sqs.receive_message(
            QueueUrl=os.getenv("QUEUE_URL", None),
            MessageSystemAttributeNames=['All'],
            MessageAttributeNames=['.*'],
            MaxNumberOfMessages=1,
            VisibilityTimeout=1200,
            WaitTimeSeconds=20,
        )
        logger.info(f"Received SQS event {response}")
        print(f"Received SQS event {response}", flush=True)
        if response and 'Messages' in response:
            receipt = response['Messages'][0]['ReceiptHandle']
            event = json.loads(response['Messages'][0]['Body'])
            if 'Records' in event and 's3' in event['Records'][0]:
                try:
                    bucket = event['Records'][0]['s3']['bucket']['name']
                    key = event['Records'][0]['s3']['object']['key']
                    logger.info(f"Received Event from Bucket {bucket}, File {key}")
                    print(f"Received Event from Bucket {bucket}, File {key}", flush=True)
                    if "report.csv" not in key:
                        split_response = split_csv_into_parquet(bucket, key)
                        fire_parquet_done(split_response)
                    else:
                        logger.warn("not processing report.csv: %s", key)

                    # delete message after successfully processing the file
                    response = sqs.delete_message(QueueUrl=os.getenv("QUEUE_URL", None), ReceiptHandle=receipt)

                except Exception as e:
                    logger.exception("Error processing S3 SQS message event.", e)
            else:
                # if a message comes in that isn't part of our S3 events, delete from queue
                response = sqs.delete_message(QueueUrl=os.getenv("QUEUE_URL", None), ReceiptHandle=receipt)


if __name__ == '__main__':
    watch_queue()
