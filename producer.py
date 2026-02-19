from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random
import os

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'test-topic')
INTERVAL = int(os.getenv('INTERVAL', '5'))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

counter = 0
while True:
    counter += 1
    message = {
        'id': counter,
        'timestamp': datetime.now().isoformat(),
        'message': f'Mock event {counter}',
        'random_value': random.randint(1, 100)
    }
    
    producer.send(TOPIC, value=message)
    producer.flush()
    print(f"Sent: {message}")
    time.sleep(INTERVAL)
