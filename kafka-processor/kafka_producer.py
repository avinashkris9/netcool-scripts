#!/usr/bin/env python3
import json
import time
from datetime import datetime

from kafka import KafkaProducer

# Kafka Configuration
KAFKA_BROKER = "0.0.0.0:9092"  # Change to match your setup
TOPIC = "FixedDomain"

# Sample message template


def generate_sample_message(device_name="Router-1", interface="eth0"):
    timestamp = int(time.time())
    return {
        "collectionTimeEpoch": timestamp,
        "networkElementPath": f"abcd > xyz > blah > blahbal > {device_name} > {interface}",
        "recordTypeName": "interface",
        "data": [
            {"name": "Devices Name", "value": device_name},
            {"name": "Interface Name", "value": interface},
            {"name": "Octets In", "value": 123456},
            {"name": "Octets Out", "value": 654321},
            {"name": "Throughput In", "value": 100.5},
            {"name": "Throughput Out", "value": 80.2},
            {"name": "Bandwidth", "value": 1000},
            {"name": "Utilization In", "value": 10.5},
            {"name": "Utilization Out", "value": 8.1},
            {"name": "Interface Availability", "value": 99.9},
            {"name": "Errors In", "value": 2},
            {"name": "Errors Out", "value": 1},
        ],
    }


def send_messages(num_messages=10, delay=1):
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for i in range(num_messages):
        msg = generate_sample_message(device_name=f"Router-{i}", interface="eth0")
        producer.send(TOPIC, value=msg)
        print(f"[{datetime.now()}] Sent message {i + 1}")
        time.sleep(delay)

    producer.flush()
    producer.close()
    print("All messages sent.")


if __name__ == "__main__":
    send_messages(num_messages=200, delay=0)
