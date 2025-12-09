import json
import time
import logging
import psycopg2
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError, Producer
from pydantic import ValidationError
from eugrid_monitor_core.service import ServiceWorker
from eugrid_monitor_core.topics import DLQ_STORAGE
from eugrid_monitor_core.models import DlqStorageEvent
from .config import settings
from .repository import PostgresRepo

class StorageWorker(ServiceWorker):
    def __init__(self):
        self._consumer = None
        self._producer = None
        self._db_connection = None
        self._repo = None

        self._event_buffers = {topic: [] for topic in settings.DB_MAPPINGS.keys()}
        self._last_flush_time = time.time()
    
    def startup(self) -> None:
        
        logging.info("Connecting to db...")
        self._db_connection = psycopg2.connect(
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            dbname=settings.DB_NAME,
        )
        logging.info("Connected to db.")

        self._repo = PostgresRepo(self._db_connection)
        
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
            "group.id": settings.KAFKA_GROUP_ID,
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest"
        })
        self._consumer.subscribe(list(settings.DB_MAPPINGS.keys()))
        logging.info("Kafka connected.")        

    def run_cycle(self) -> None:
        """Consumes enriched events from Kafka and uploads them to the db."""

        msg = self._consumer.poll(timeout=1.0)
        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logging.error(f"Kafka error: {msg.error()}")
                else:
                    topic = msg.topic()
                    try:
                        # Load the raw event and add it to the buffers
                        raw_message = json.loads(msg.value().decode("utf-8"))
                        model = settings.DB_MAPPINGS[topic].model
                        event = model.model_validate(raw_message)
                        self._event_buffers[topic].append(event.model_dump(mode="json"))
                    except (json.JSONDecodeError, ValidationError) as e: 
                        self._handle_dlq(msg, e)
                        self._consumer.commit(message=msg, asynchronous=False)

        # If more than 10 seconds have passed, or the buffer limit is surpassed, flush the buffers
        current_time = time.time()
        total_events = sum(len(buf) for buf in self._event_buffers.values())

        should_flush_size = total_events >= settings.MAX_BATCH_SIZE
        should_flush_time = (current_time - self._last_flush_time) >= settings.MAX_BATCH_INTERVAL_SECONDS

        if total_events > 0 and (should_flush_size or should_flush_time):
            self._flush_buffers()

    def _flush_buffers(self):
        """Writes the buffered events to the db."""
        for topic, events in self._event_buffers.items():
            if not events:
                continue

            config = settings.DB_MAPPINGS[topic]
            try:
                self._repo.bulk_insert(
                    self._db_connection, 
                    config.table_name, 
                    config.columns, 
                    config.conflict_columns, 
                    events
                )
                self._event_buffers[topic] = []

            except Exception as e:
                logging.error(f"DB insert failed for {topic}: {e}")
                raise e

        self._consumer.commit(asynchronous=False)
        self._last_flush_time = time.time()
        
    def _handle_dlq(self, msg, error):
        """Uploads a failed msg to the DLQ, along with its error details."""
        logging.error(f"Validation failed: {error}")
        try:
            dlq_event = DlqStorageEvent(
                failed_at=datetime.now(timezone.utc),
                error_msg=str(error),
                error_type=type(error).__name__,
                original_message=msg.value()
            )
            self._producer.produce(DLQ_STORAGE, value=dlq_event.model_dump_json())
            self._producer.poll(0)
        except Exception:
            logging.error("Failed to produce to DLQ", exc_info=True)

    def shutdown(self) -> None:
        logging.info("Shutting down storage worker...")
        if any(self._event_buffers.values()):
            self._flush_buffers()
        if self._db_connection:
            self._db_connection.close()
        if self._consumer:
            self._consumer.close()
        if self._producer:
            self._producer.flush()
