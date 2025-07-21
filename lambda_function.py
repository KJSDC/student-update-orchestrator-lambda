import json
import boto3
import concurrent.futures

import logging

from constants import *
from lambda_utils import split_batches, extract_rows_from_event, invoke_child_lambda

logger = logging.getLogger()
logger.setLevel(logging.INFO)

LAMBDA_CLIENT = boto3.client("lambda")

def lambda_handler(event, context):
    logger.info("Parent lambda: triggered")

    # Step 1: Parse XLSX and extract all rows (as list of dicts)
    rows = extract_rows_from_event(event)
    if not rows:
        return {
            "statusCode": 400,
            "headers": RESPONSE_HEADERS,
            "body": json.dumps({
                "success": False,
                "message": "No records found in uploaded file"
            })
        }

    # Step 2: Split into batches
    batches = list(split_batches(rows, BATCH_SIZE))
    logger.info(f"Split into {len(batches)} batch(es)")

    # Step 3: Invoke child Lambdas concurrently
    all_failed = []

    def invoke_with_env(batch):
        # Pass in the Lambda client and child lambda name
        return invoke_child_lambda(
            lambda_client=LAMBDA_CLIENT,
            child_lambda_name=CHILD_LAMBDA_NAME,
            batch=batch
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(invoke_with_env, batch) for batch in batches]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if not result.get("success"):
                all_failed.extend(result.get("failedRows", []))

    if all_failed:
        logger.warning(f"Failed rows: {all_failed}")
        return {
            "statusCode": 207,
            "headers": RESPONSE_HEADERS,
            "body": json.dumps({
                "success": False,
                "message": "Partial failure",
                "failedRows": all_failed
            })
        }

    logger.info("All child Lambdas completed successfully")
    return {
        "statusCode": 200,
        "headers": RESPONSE_HEADERS,
        "body": json.dumps({
            "success": True,
            "message": "All records processed successfully"
        })
    }