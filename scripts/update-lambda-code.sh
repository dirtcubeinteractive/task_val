#!/bin/bash
# Update the Lambda function with the new code package

# Set the variables
LAMBDA_FUNCTION_NAME="24-wallet_balance_updated"
S3_BUCKET_NAME="specter-deploy"
S3_KEY="deploy.zip"

# Update the Lambda function code
aws lambda update-function-code --function-name ${LAMBDA_FUNCTION_NAME} --s3-bucket ${S3_BUCKET_NAME} --s3-key ${S3_KEY}
