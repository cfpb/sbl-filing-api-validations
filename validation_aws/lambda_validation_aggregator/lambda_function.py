import logging
import urllib.parse

from sbl_validation_processor.results_aggregator import aggregate_validation_results

log = logging.getLogger()
log.setLevel(logging.INFO)

def lambda_handler(event, context):
    request = event['responsePayload'] if 'responsePayload' in event else event

    bucket = request['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(request['Records'][0]['s3']['object']['key'], encoding='utf-8')
    log.info(f"Received key: {key}")

    try:
        aggregate_validation_results(bucket, key)
    except Exception as e:
        log.exception('Failed to validate {} in {}'.format(key, bucket))
        raise e
