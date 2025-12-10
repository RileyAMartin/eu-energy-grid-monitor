import json
import logging
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaError
from eugrid_monitor_core.service import ServiceWorker
from eugrid_monitor_core.models import DlqProcessingEvent
from eugrid_monitor_core.topics import DLQ_PROCESSING
from .config import settings

class ProcessingWorker(ServiceWorker):
    def __init__(self):
        self._consumer = None
        self._producer = None

    def startup(self) -> None:
        logging.info("Connecting to Kafka...")
        producer_config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": settings.KAFKA_SASL_USERNAME,
            "sasl.password": settings.KAFKA_SASL_PASSWORD,
        }
        self._producer = Producer(producer_config)

        self._consumer = Consumer({
            **producer_config,
            "group.id": "processor-v1",
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest"
        })
        self._consumer.subscribe(settings.RAW_TOPICS)
        logging.info("Kafka connected.")

    def run_cycle(self) -> None:
        """Polls for one message and processes it."""
        msg = self._consumer.poll(timeout=1.0)
        
        if msg is None: 
            return

        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                logging.error(f"Kafka consumer error: {msg.error()}")
            return

        # Process the valid message
        topic = msg.topic()
        processor_config = settings.PROCESSING_DISPATCHER.get(topic)

        if not processor_config:
            logging.warning(f"No processor configured for topic: {topic}")
            self._consumer.commit(message=msg, asynchronous=False)
            return

        try:
            # Decode the raw event from Kafka
            model = processor_config["model"]
            raw_event = model.model_validate(json.loads(msg.value().decode("utf-8")))
            
            # Convert the raw event to a list of enriched events 
            processing_function = processor_config["processing_function"]
            fn_kwargs = processor_config.get("kwargs", {})
            enriched_events = processing_function(
                raw_event,
                **fn_kwargs
            )

            # Produce the enriched events to Kafka
            for event in enriched_events:
                self._produce_safe(
                    processor_config["enriched_topic"],
                    key=event.eic_code.encode("utf-8"),
                    value=event.model_dump_json()
                )
            self._consumer.commit(message=msg, asynchronous=False)

        except Exception as e:
            self._handle_error(msg, e)
            self._consumer.commit(message=msg, asynchronous=False)

    def _produce_safe(self, topic, key, value):
        """Polls after producing an event to make it safer."""
        while True:
            try:
                self._producer.produce(topic, key=key, value=value)
                self._producer.poll(0)
                break
            except BufferError:
                self._producer.poll(1)

    def _handle_error(self, msg, error):
        """Sends failed messages to the DLQ."""
        logging.error(f"Processing failed: {error}")
        try:
            dlq_event = DlqProcessingEvent(
                failed_at=datetime.now(timezone.utc),
                error_msg=str(error),
                error_type=type(error).__name__,
                original_message=msg.value()
            )
            self._produce_safe(DLQ_PROCESSING, key=None, value=dlq_event.model_dump_json())
        except Exception as dlq_e:
            logging.error(f"DLQ failed: {dlq_e}", exc_info=True)

    def shutdown(self) -> None:
        logging.info("Shutting down processor...")
        if self._producer:
            self._producer.flush()
        if self._consumer:
            self._consumer.close()
