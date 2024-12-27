import argparse
import os
import boto3
import json
import logging

from sbl_validation_processor.results_aggregator import aggregate_validation_results

logger = logging.getLogger()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Parquet Aggregator Job")
    parser.add_argument("--bucket")
    parser.add_argument("--key")
    args = parser.parse_args()
    print(args)
    if not args.bucket or not args.key:
        logger.error("Error running parquet aggregator job.  --bucket and --key must be present.")
    else:
        aggregate_validation_results(args.bucket, args.key)