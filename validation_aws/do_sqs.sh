#!/bin/bash
run_in_docker="$1"
echo $run_in_docker
if [ ! -z "$run_in_docker" ]
then
    docker kill local_validation_run
    docker rm local_validation_run
    docker build --platform linux/amd64 -t local_validation_run:latest -f Local_Dockerfile .
    docker run -d --platform linux/amd64 --network sbl-project_default -v /tmp/filing_bucket/upload/:/tmp/filing_bucket/upload/ -e ENV=LOCAL -e DB_NAME=filing -e DB_USER=filing_user -e DB_PWD=filing_user -e DB_HOST=pg --name local_validation_run local_validation_run:latest
    docker container ls
else
    docker build --platform linux/amd64 -t sqs-parquet:latest -f SQS_Dockerfile --build-arg SQS_PATH=sqs_csv_to_parquet .
    docker build --platform linux/amd64 -t sqs-validate:latest -f SQS_Dockerfile --build-arg SQS_PATH=sqs_parquet_validation .
    docker build --platform linux/amd64 -t sqs-aggregator:latest -f SQS_Dockerfile --build-arg SQS_PATH=sqs_validation_aggregator .
    docker build --platform linux/amd64 -t sqs-parquet-job:latest -f Job_Dockerfile --build-arg JOB_PATH=sqs_csv_to_parquet .
    docker build --platform linux/amd64 -t sqs-validator-job:latest -f Job_Dockerfile --build-arg JOB_PATH=sqs_parquet_validation .
    docker build --platform linux/amd64 -t sqs-aggregator-job:latest -f Job_Dockerfile --build-arg JOB_PATH=sqs_validation_aggregator .
    docker tag sqs-parquet:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-parquet:latest
    docker tag sqs-parquet-job:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-parquet-job:latest
    docker tag sqs-validate:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-validate:latest
    docker tag sqs-validator-job:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-validator-job:latest
    docker tag sqs-aggregator:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-aggregator:latest
    docker tag sqs-aggregator-job:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-aggregator-job:latest
    docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-parquet:latest
    docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-parquet-job:latest
    docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-validate:latest
    docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-validator-job:latest
    docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-aggregator:latest
    docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-aggregator-job:latest
fi

