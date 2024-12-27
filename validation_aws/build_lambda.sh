#!/bin/bash

while getopts p:t: option; do
    case $option in
        p) path=$OPTARG ;;
        t) tag=$OPTARG ;;
        *) echo "Usage: $0 -p <lambda_path> -t <image_tag>" >&2
           exit 1 ;;
    esac
done
if [[ -z $path ||  -z $tag ]]; then
    echo "Usage: $0 -p <lambda_path> -t <tag>" >&2
    exit 1
fi
docker build --platform linux/amd64 -f Lambda_Dockerfile -t $tag --build-arg LAMBDA_PATH=$path .