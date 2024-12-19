#!/bin/bash
docker kill sqs-validate sqs-parquet sqs-aggregator
docker container rm sqs-validate sqs-parquet sqs-aggregator
docker image rm sqs-validate sqs-parquet sqs-aggregator
docker build --platform linux/amd64 -t sqs-parquet:latest -f SQS_Dockerfile --build-arg SQS_PATH=sqs_csv_to_parquet .
docker build --platform linux/amd64 -t sqs-validate:latest -f SQS_Dockerfile --build-arg SQS_PATH=sqs_parquet_validation .
docker build --platform linux/amd64 -t sqs-aggregator:latest -f SQS_Dockerfile --build-arg SQS_PATH=sqs_validation_aggregator .
docker build --platform linux/amd64 -t sqs-validator-job:latest -f Job_Dockerfile .
docker tag sqs-parquet:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-parquet:latest
docker tag sqs-validate:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-validate:latest
docker tag sqs-validator-job:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-validator-job:latest
docker tag sqs-aggregator:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-aggregator:latest
docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-parquet:latest
docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-validate:latest
docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-validator-job:latest
docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-aggregator:latest

run_in_docker="$1"
echo $run_in_docker
if [ ! -z "$run_in_docker"]; then
    docker run -d --platform linux/amd64 -e AWS_PROFILE=$AWS_PROFILE -e QUEUE_URL=https://sqs.us-east-1.amazonaws.com/099248080076/cfpb-regtech-dev-s3-queue-test -v ~/.aws/:/root/.aws/:ro  --name sqs-parquet sqs-parquet:latest
    docker run -d --platform linux/amd64 -e AWS_PROFILE=$AWS_PROFILE -e QUEUE_URL=https://sqs.us-east-1.amazonaws.com/099248080076/cfpb-regtech-dev-pqs-validate -v ~/.aws/:/root/.aws/:ro  --name sqs-validate sqs-validate:latest
    docker run -d --platform linux/amd64 -e AWS_PROFILE=$AWS_PROFILE -e QUEUE_URL=https://sqs.us-east-1.amazonaws.com/099248080076/cfpb-regtech-dev-res-aggregate -v ~/.aws/:/root/.aws/:ro  --name sqs-aggregator sqs-aggregator:latest
    docker container ls
fi

