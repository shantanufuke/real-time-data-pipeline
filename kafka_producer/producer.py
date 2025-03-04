from kafka import KafkaProducer
import json
import time
import random

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "data_stream"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulated data generator
def generate_data():
    return {
        "id": random.randint(1, 100),
        "value": random.uniform(10.5, 99.9),
        "timestamp": time.time()
    }

# Send data to Kafka topic
while True:
    data = generate_data()
    producer.send(TOPIC_NAME, value=data)
    print(f"Sent: {data}")
    time.sleep(2)  # Simulate data arrival every 2 seconds
