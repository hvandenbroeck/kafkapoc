from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import json
import time
from datetime import datetime
import random
import os
from faker import Faker

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'incoming_messages')
INTERVAL = int(os.getenv('INTERVAL', '5'))
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')

fake = Faker()

# Load Avro schema
with open('incoming_messages.avsc', 'r') as f:
    schema_str = f.read()

# Configure Schema Registry client
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Create AvroSerializer
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Configure Producer
producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def generate_fake_message():
    # Generate realistic fake data
    num_strings = random.randint(1, 5)
    num_ints = random.randint(1, 3)
    num_datetimes = random.randint(0, 2)
    
    message = {
        'hene_id': fake.uuid4(),
        'deliveryTemplate': random.choice(['welcome_email', 'password_reset', 'invoice', 'newsletter', 'promotion']),
        'partyid': fake.uuid4(),
        'brand': random.choice(['BrandA', 'BrandB', 'BrandC']),
        'email': fake.email(),
        'firstName': fake.first_name(),
        'lastName': fake.last_name(),
        'language': random.choice(['en', 'nl', 'fr', 'de', 'es'])
    }
    
    # Add some random string fields
    for i in range(1, num_strings + 1):
        message[f'string{i}'] = fake.word()
    for i in range(num_strings + 1, 11):
        message[f'string{i}'] = None
    
    # Add some random int fields
    for i in range(1, num_ints + 1):
        message[f'int{i}'] = random.randint(1, 1000)
    for i in range(num_ints + 1, 11):
        message[f'int{i}'] = None
    
    # Add some random datetime fields (as timestamp in milliseconds)
    for i in range(1, num_datetimes + 1):
        message[f'datetime{i}'] = int(fake.date_time_between(start_date='-1y', end_date='now').timestamp() * 1000)
    for i in range(num_datetimes + 1, 11):
        message[f'datetime{i}'] = None
    
    return message

counter = 0
print(f"Starting producer - sending to topic '{TOPIC}'")
print(f"Schema Registry URL: {SCHEMA_REGISTRY_URL}")

while True:
    try:
        counter += 1
        message = generate_fake_message()
        
        # Serialize and send
        producer.produce(
            topic=TOPIC,
            value=avro_serializer(message, SerializationContext(TOPIC, MessageField.VALUE)),
            on_delivery=delivery_report
        )
        
        producer.poll(0)
        print(f"Sent message {counter}: {message['hene_id']} - {message['email']}")
        time.sleep(INTERVAL)
    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)

producer.flush()