from confluent_kafka import Consumer, KafkaError, Producer
from pydantic import ValidationError
from config import settings
from eugrid_monitor_core.models import RawGenerationEvent, EventJSONDecoder
import eugrid_monitor_core.topics as topics
from .processors.generation import process_generation_event
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

    # Processing functions and output topics for each raw data topic in the queue
    processing_dispatcher = {
        topics.RAW_GENERATION_EVENTS: {
            "enriched_topic": topics.ENRICHED_GENERATION_EVENTS,
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

            # Start processing the message
            topic = msg.topic()
            processor_config = processing_dispatcher.get(topic)

            if not processor_config:
                logging.warning(f"No processor configured for topic: {topic}")
                consumer.commit(message=msg, asynchronous=False)
                continue
            
            try:
                # Convert the raw event to a sequence of enriched events
                raw_message = json.loads(msg.value().decode("utf-8"))
                raw_event = RawGenerationEvent.model_validate(raw_message)
                enriched_events = processor_config["processing_function"](raw_event)

                # Produce the enriched events
                for event in enriched_events:
                    event_dict = event.model_dump(mode="json")
                    producer.produce(
                        processor_config["enriched_topic"],
                        key=event.eic_code.encode("utf-8"),
                        value=json.dumps(event_dict, cls=EventJSONDecoder)
                    )

                # Commit the offset
                consumer.commit(message=msg, asynchronous=False)

            except (json.JSONDecodeError, ValidationError, Exception) as e:
                # On an exception, send the original message to the DLQ                
                logging.error(f"Failed to process message: {e}\nMessage Value: {msg.value()}")
                try:
                    producer.produce(
                        topics.DLQ_PROCESSING,
                        value=msg.value()
                    )
                except Exception as dlq_e:
                    logging.error(f"Couldn't produce message to DLQ. Error: {dlq_e}")
                    continue
                consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        logging.info("Shutting down consumer...")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
