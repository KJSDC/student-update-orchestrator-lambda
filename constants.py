CHILD_LAMBDA_NAME = "student-update-worker"  # Name of your child to be spawned
BATCH_SIZE = 500

# RESPONSE HEADER
RESPONSE_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,x-auth-correlation-id,content-disposition",
    "Access-Control-Allow-Methods": "OPTIONS,POST"
}

