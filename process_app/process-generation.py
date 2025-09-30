import datetime
import json
import os
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv
from eic_codes import EIC_CODES_GENERATION

load_dotenv()

# Kafka Setup
GENERATION_DATA_TOPIC = "generation-events-raw"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")
if not KAFKA_BOOTSTRAP_SERVERS or not KAFKA_SASL_USERNAME or not KAFKA_SASL_PASSWORD:
    raise ValueError("Kafka config variables not found in .env.")
kafka_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": KAFKA_SASL_USERNAME,
    "sasl.password": KAFKA_SASL_PASSWORD,
    "group.id": "kafka-python-client",
    "enable.auto.commit": "false",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(kafka_config)

# Basic consumer loop
running = True
try:
    consumer.subscribe([GENERATION_DATA_TOPIC])

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("Reached end of file.")
        
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            try:
                msg_json = json.loads(msg.value().decode("utf-8"))
                print(msg_json)
            except:
                print("Couldn't decode.")
                print(msg.value())

finally:
    consumer.close()