import os
import boto3
import json
import logging

from datetime import datetime
from kubernetes import client, config

logger = logging.getLogger()
logger.setLevel("INFO")


def watch_queue():
    region_name = "us-east-1"

    session = boto3.session.Session()
    sqs = session.client(service_name="sqs", region_name=region_name)

    while True:
        response = sqs.receive_message(
            QueueUrl=os.getenv("QUEUE_URL", None),
            MessageSystemAttributeNames=["All"],
            MessageAttributeNames=[".*"],
            MaxNumberOfMessages=1,
            VisibilityTimeout=1200,
            WaitTimeSeconds=20,
        )
        logger.info(f"Received SQS event {response}")
        if response and "Messages" in response:
            receipt = response["Messages"][0]["ReceiptHandle"]
            event = json.loads(response["Messages"][0]["Body"])
            if "Records" in event and "s3" in event["Records"][0]:
                try:
                    bucket = event["Records"][0]["s3"]["bucket"]["name"]
                    key = event["Records"][0]["s3"]["object"]["key"]
                    logger.info(f"Received Event from Bucket {bucket}, File {key}")
                    if "report.csv" not in key:
                        paths = key.split("/")
                        sub_id = paths[-1].split(".")[0]

                        fire_k8s_job(bucket, key, f"{sub_id}-{paths[-2]}-{paths[-3]}")
                    else:
                        logger.warn("not processing report.csv: %s", key)

                    # delete message after successfully processing the file
                    response = sqs.delete_message(
                        QueueUrl=os.getenv("QUEUE_URL", None), ReceiptHandle=receipt
                    )

                except Exception as e:
                    logger.exception("Error processing S3 SQS message event.", e)
            else:
                # if a message comes in that isn't part of our S3 events, delete from queue
                response = sqs.delete_message(
                    QueueUrl=os.getenv("QUEUE_URL", None), ReceiptHandle=receipt
                )


def fire_k8s_job(bucket: str, key: str, job_id: str):
    config.load_incluster_config()
    batch_v1 = client.BatchV1Api()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    job = client.V1Job(
        metadata=client.V1ObjectMeta(
            name=f"parquet-job-{timestamp}", annotations={"job-id": job_id}
        ),
        spec=client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    containers=[
                        client.V1Container(
                            name=f"parquet-job-{timestamp}",
                            image=os.getenv("JOB_IMAGE"),
                            command=["python", "job.py"],
                            args=["--bucket", bucket, "--key", key],
                            env=[
                                client.V1EnvVar(
                                    name="EVENT_BUS", value=os.getenv("EVENT_BUS")
                                )
                            ],
                        )
                    ],
                    restart_policy="Never",
                    service_account_name="cfpb-ci-sa-sqs",
                )
            ),
            backoff_limit=3,
            # keep jobs around for a day before deleting
            ttl_seconds_after_finished=86400,
        ),
    )

    batch_v1.create_namespaced_job(namespace="regtech", body=job)


if __name__ == "__main__":
    watch_queue()
