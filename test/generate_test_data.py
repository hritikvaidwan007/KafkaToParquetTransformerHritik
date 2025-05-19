#!/usr/bin/env python
"""
This script generates sample data and sends it to a Kafka topic for testing the Flink application.
It creates messages that comply with the Avro schema from the sample API response.
"""

import json
import time
import random
import argparse
from datetime import datetime
from kafka import KafkaProducer

def generate_event():
    """Generate a random TV event that matches the Avro schema"""
    timestamp = datetime.now().isoformat()
    
    return {
        "_timestamp": timestamp,
        "title": f"Show {random.randint(1, 100)}",
        "schemaId": random.randint(1, 10),
        "schemaVersion": random.randint(1, 5)
    }

def send_to_kafka(bootstrap_servers, topic, num_events, delay=1):
    """Send events to Kafka topic"""
    print(f"Connecting to Kafka at {bootstrap_servers}")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for i in range(num_events):
        event = generate_event()
        producer.send(topic, event)
        print(f"Sent event {i+1}/{num_events}: {event}")
        time.sleep(delay)
    
    producer.flush()
    producer.close()
    print(f"Finished sending {num_events} events to topic {topic}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate test data for Kafka")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="input-topic", help="Kafka topic")
    parser.add_argument("--events", type=int, default=50, help="Number of events to generate")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between events in seconds")
    
    args = parser.parse_args()
    
    send_to_kafka(args.bootstrap_servers, args.topic, args.events, args.delay)