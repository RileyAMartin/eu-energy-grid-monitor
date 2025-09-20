import datetime
import json
import os
from confluent_kafka import Consumer
from eic_codes import EIC_CODES_GENERATION

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
    "enable.auto.commit": "false",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(kafka_config)

for message in consumer:
    print(message.value)



