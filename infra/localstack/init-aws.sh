#!/bin/bash
  set -e

  export AWS_ACCESS_KEY_ID=test
  export AWS_SECRET_ACCESS_KEY=test
  export AWS_DEFAULT_REGION=us-east-1

  aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name raw-events-dlq
  aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name raw-events \
    --attributes '{"RedrivePolicy": "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:raw-events-dlq\",\"maxReceiveCount\":3}"}'

  aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name processed-events-dlq
  aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name processed-events \
    --attributes '{"RedrivePolicy": "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:processed-events-dlq\",\"maxReceiveCount\":3}"}'

  aws --endpoint-url=http://localhost:4566 dynamodb create-table \
    --table-name events \
    --attribute-definitions \
      AttributeName=event_id,AttributeType=S \
      AttributeName=developer_id,AttributeType=S \
    --key-schema AttributeName=event_id,KeyType=HASH \
    --global-secondary-indexes \
      'IndexName=developer_id-index,KeySchema=[{AttributeName=developer_id,KeyType=HASH}],Projection={ProjectionType=ALL}' \
    --billing-mode PAY_PER_REQUEST

  aws --endpoint-url=http://localhost:4566 dynamodb create-table \
    --table-name developer_summary \
    --attribute-definitions AttributeName=developer_id,AttributeType=S \
    --key-schema AttributeName=developer_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
