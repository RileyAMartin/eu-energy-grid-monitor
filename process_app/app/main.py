from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from config import settings
from models import RawGenerationEvent, EventJSONDecoder
from datetime import datetime
from processors.generation import process_generation_event
import logging
import json

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("--- Starting processing ---")

    # Kafka Setup
    producer_config = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": settings.KAFKA_SASL_USERNAME,
        "sasl.password": settings.KAFKA_SASL_PASSWORD,
    }
    consumer_config = {
        **producer_config,
        "group.id": "processor-v1",
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest"
    }
    producer = Producer(producer_config)
    consumer = Consumer(consumer_config)

    # Processing functions and output topics for each raw data topic in the Queue
    processing_dispatcher = {
        settings.RAW_GENERATION_TOPIC: {
            "enriched_topic": settings.ENRICHED_GENERATION_TOPIC,
            "processing_function": process_generation_event
        }
    }

    # Enrich raw events
    consumer.subscribe(list(processing_dispatcher.keys()))
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logging.error(f"Kafka consumer error: {msg.error()}")
                continue

            try:
                topic = msg.topic()
                raw_message = json.loads(msg.value().decode("utf-8"))

                # Enrich the raw events according to their topic
                processor_config = processing_dispatcher.get(topic)
                if not processor_config:
                    logging.warning(f"No processor configured for topic: {topic}")
                    continue
                raw_event = RawGenerationEvent.model_validate(raw_message)
                enriched_events = processor_config["processing_function"](raw_event)

                # Produce the enriched events
                for event in enriched_events:
                    event_dict = event.model_dump(mode="JSON")
                    producer.produce(
                        processor_config["enriched_topic"],
                        key=event.eic_code.encode("utf-8"),
                        value=json.dumps(event_dict, cls=EventJSONDecoder)
                    )

                # Commit the offset
                consumer.commit(asynchronous=False)

            except (json.JSONDecodeError, Exception) as e:
                logging.error(f"Failed to process message: {e}")
                consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        logging.info("Shutting down consumer...")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
