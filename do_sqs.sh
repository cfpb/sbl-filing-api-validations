#!/bin/bash
while getopts e:t: option; do
    case $option in
        e) env=$OPTARG ;;
        t) tag=$OPTARG ;;
        *) echo "Usage: $0 -e <[LOCAL|REMOTE]]> -t <image_tag>" >&2
           exit 1 ;;
    esac
done
if [[ -z $env ||  -z $tag ]]; then
    echo "Usage: $0 -e <[LOCAL|REMOTE]]> -t <image_tag>" >&2
    exit 1
fi
if [ $env == 'LOCAL' ]
then
    docker kill local_validation_run
    docker rm local_validation_run
    docker build --platform linux/amd64 -t local_validation_run:latest -f Local_Dockerfile .
    docker run -d --platform linux/amd64 --network sbl-project_default -v /tmp/filing_bucket/upload/:/tmp/filing_bucket/upload/ -e ENV=LOCAL -e DB_NAME=filing -e DB_USER=filing_user -e DB_PWD=filing_user -e DB_HOST=pg --name local_validation_run local_validation_run:latest
    docker container ls
else
    docker build --platform linux/amd64 -t sqs-parquet:latest -f SQS_Dockerfile --build-arg SQS_PATH=src/sbl_validation_processor/sqs_csv_to_parquet .
    docker build --platform linux/amd64 -t sqs-validate:latest -f SQS_Dockerfile --build-arg SQS_PATH=src/sbl_validation_processor/sqs_parquet_validation .
    docker build --platform linux/amd64 -t sqs-aggregator:latest -f SQS_Dockerfile --build-arg SQS_PATH=src/sbl_validation_processor/sqs_validation_aggregator .
    docker build --platform linux/amd64 -t sqs-parquet-job:latest -f Job_Dockerfile --build-arg JOB_PATH=src/sbl_validation_processor/sqs_csv_to_parquet .
    docker build --platform linux/amd64 -t sqs-validator-job:latest -f Job_Dockerfile --build-arg JOB_PATH=src/sbl_validation_processor/sqs_parquet_validation .
    docker build --platform linux/amd64 -t sqs-aggregator-job:latest -f Job_Dockerfile --build-arg JOB_PATH=src/sbl_validation_processor/sqs_validation_aggregator .
    docker tag sqs-parquet:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-parquet:$tag
    docker tag sqs-parquet-job:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-parquet-job:$tag
    docker tag sqs-validate:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-validate:$tag
    docker tag sqs-validator-job:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-validator-job:$tag
    docker tag sqs-aggregator:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-aggregator:$tag
    docker tag sqs-aggregator-job:latest 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-aggregator-job:$tag
    docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-parquet:$tag
    docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-parquet-job:$tag
    docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-validate:$tag
    docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-validator-job:$tag
    docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-aggregator:$tag
    docker push 099248080076.dkr.ecr.us-east-1.amazonaws.com/cfpb/regtech/sqs-aggregator-job:$tag
    kubectl rollout restart deployment -n regtech sqs-csv-poller
    kubectl rollout restart deployment -n regtech sqs-pqs-poller
    kubectl rollout restart deployment -n regtech sqs-res-poller
fi

