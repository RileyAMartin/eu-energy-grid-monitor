import logging
import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError, Producer
from eugrid_monitor_core.models import RawGenerationEvent, DlqProcessingEvent
from eugrid_monitor_core.topics import DLQ_PROCESSING
from .config import settings

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

    # Enrich raw events
    consumer.subscribe(settings.RAW_TOPICS)
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
            processor_config = settings.PROCESSING_DISPATCHER.get(topic)

            if not processor_config:
                logging.warning(f"No processor configured for topic: {topic}")
                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                # Convert the raw event to a sequence of enriched events
                model = processor_config["model"]
                processing_function = processor_config["processing_function"]
                raw_event = model.model_validate(json.loads(msg.value().decode("utf-8")))
                enriched_events = processing_function(
                    raw_event,
                    settings.PSR_TYPE_MAPPINGS,
                    settings.EIC_MAPPINGS
                )

                # Produce the enriched events
                for event in enriched_events:
                    try:
                        producer.produce(
                            processor_config["enriched_topic"],
                            key=event.eic_code.encode("utf-8"),
                            value=event.model_dump_json()
                        )
                    except BufferError:
                        logging.warning(f"Local producer queue is full. Flushing queue before continuing.")
                        producer.flush()
                        logging.warning("Local producer flushed. Retrying the message.")
                        producer.produce(
                            processor_config["enriched_topic"],
                            key=event.eic_code.encode("utf-8"),
                            value=event.model_dump_json()
                        )
                    producer.poll(0)

                # Commit the offset
                consumer.commit(message=msg, asynchronous=False)

            # On an exception, send the original message to the DLQ
            except Exception as e:
                logging.error(f"Failed to process message: {e}\nMessage Value: {msg.value()}")
                try:
                    dlq_event = DlqProcessingEvent(
                        failed_at=datetime.now(timezone.utc),
                        error_msg=str(e),
                        error_type=type(e).__name__,
                        original_message=msg.value()
                    )
                    try:
                        producer.produce(
                            topic=DLQ_PROCESSING,
                            value=dlq_event.model_dump_json()
                        )
                    except BufferError:
                        logging.warning(f"Local producer queue is full. Flushing before continuing.")
                        producer.flush()
                        logging.warning("Local producer flushed. Retrying the message.")
                        producer.produce(
                            topic=DLQ_PROCESSING,
                            value=dlq_event.model_dump_json()
                        )
                except Exception as dlq_e:
                    logging.error(f"Couldn't produce message to DLQ. Error: {dlq_e}", exc_info=True)
                    continue
                consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        logging.info("Shutting down consumer...")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
