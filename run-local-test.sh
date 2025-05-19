#!/bin/bash

# Script to run the Flink application locally with test data

# Set environment variables for S3 access (MinIO)
export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=minio123
export AWS_REGION=us-east-1
export S3_ENDPOINT=http://localhost:9000

# Make sure the script is executable
chmod +x test/generate_test_data.py

# Create output directory
mkdir -p /tmp/output

echo "Starting Flink LocalTestJob..."
# Run the Flink application with local path output for easier testing
java -classpath target/classes:target/dependency/* com.example.LocalTestJob \
  --bootstrap-servers localhost:9092 \
  --topic input-topic \
  --output-path file:///tmp/output \
  --window-minutes 1

# To use S3 output instead:
# java -classpath target/classes:target/dependency/* com.example.LocalTestJob \
#   --bootstrap-servers localhost:9092 \
#   --topic input-topic \
#   --output-path s3://my-bucket/output \
#   --window-minutes 1