import argparse
import json
import logging

from sbl_validation_processor.results_aggregator import aggregate_validation_results

logger = logging.getLogger()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parquet Aggregator Job")
    parser.add_argument("--bucket")
    parser.add_argument("--key")
    parser.add_argument("--results")
    args = parser.parse_args()
    if not args.bucket or not args.key or not args.results:
        logger.error(
            "Error running parquet aggregator job.  --bucket, --key, and --results must be present."
        )
    else:
        aggregate_validation_results(args.bucket, args.key, json.loads(args.results))
