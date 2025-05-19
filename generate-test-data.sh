#!/bin/bash

# Script to generate test data and publish to Kafka
# This uses the Python script to send sample data to the Kafka topic

echo "Generating test data and sending to Kafka..."

# Run the Python script to generate events and send to Kafka
python test/generate_test_data.py \
    --bootstrap-servers localhost:9092 \
    --topic input-topic \
    --events 100 \
    --delay 0.1

echo "Finished sending 100 test events to Kafka."