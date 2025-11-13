import json
import sys
import time
import psycopg2
import logging
from confluent_kafka import Consumer, KafkaError, Producer
from pydantic import ValidationError
from datetime import datetime, timezone
from .config import settings
from .utils import perform_bulk_insert
from eugrid_monitor_core.topics import DLQ_STORAGE
from eugrid_monitor_core.models import DlqStorageEvent

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("--- Starting storage app ---")
    
    # Database Setup
    db_connection = None
    try:
        logging.info("--- Initialising database connection ---")
        db_connection = psycopg2.connect(
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            dbname=settings.DB_NAME,
        )
        logging.info("--- Connected to database ---")
    
    except Exception as e:
        logging.critical(f"FATAL: Couldn't connect to database on startup: {e}")
        sys.exit(1)
    
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
        "group.id": settings.KAFKA_GROUP_ID,
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest"
    }
    consumer = Consumer(consumer_config)
    producer = Producer(producer_config)
    topics = list(settings.DB_MAPPINGS.keys())

    # Main loop
    event_buffers = {topic: [] for topic in topics}  # Each topic is uploaded to the db individually
    last_flush_time = time.time()
    consumer.subscribe(topics)
    try:
        logging.info("--- Beginning main loop ---")
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue            
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logging.error(f"Kafka consumer error: {msg.error()}")
                continue
 
            # Add the event to its corresponding event buffer
            try:
                topic = msg.topic()
                raw_message  = json.loads(msg.value().decode("utf-8"))

                # Verify the schema of the event before converting it to a dict for easy upload to the DB
                model = settings.DB_MAPPINGS[topic]["model"]  # Pydantic model for this event type
                event = model.model_validate(raw_message)
                event_buffers[topic].append(event.model_dump(mode="json"))

            except (json.JSONDecodeError, ValidationError) as e:
                logging.error(f"Failed to process message: {e}")
                try:
                    dlq_event = DlqStorageEvent(
                        failed_at=datetime.now(timezone.utc),
                        error_msg=str(e),
                        error_type=type(e).__name__,
                        original_message=msg.value()
                    )

                    # Produce the original message to the DLQ
                    producer.produce(
                        DLQ_STORAGE,
                        value=msg.value()
                    )
                except Exception as dlq_e:
                    logging.critical(f"FATAL: Couldn't produce message to DLQ. Error: {dlq_e}")
                    continue
                consumer.commit(asynchronous=False)
                continue

            # If MAX_BATCH_SIZE or MAX_BATCH_INTERVAL_SECONDS have been reached then upload to db
            current_size = sum([len(buffer ) for buffer in event_buffers.values()])
            current_time = time.time()
            should_flush_by_size = current_size >= settings.MAX_BATCH_SIZE
            should_flush_by_time = (current_time - last_flush_time) >= settings.MAX_BATCH_INTERVAL_SECONDS

            if any(event_buffers.values()) and (should_flush_by_size or should_flush_by_time):
                all_commits_successful = True
                try:
                    for topic, events in event_buffers.items():

                        if len(events) == 0:
                            continue

                        # Upload the events to the database
                        table_name = settings.DB_MAPPINGS[topic]["table_name"]
                        columns = settings.DB_MAPPINGS[topic]["columns"]
                        conflict_columns = settings.DB_MAPPINGS[topic]["conflict_columns"]
                        perform_bulk_insert(db_connection, table_name, columns, conflict_columns, events)
                        event_buffers[topic] = []

                    # Commit the Kafka offset and reset the flush time                   
                    consumer.commit(asynchronous=False)
                    last_flush_time = time.time()

                except Exception as e:
                    logging.error("Database insert failed.")
                    all_commits_successful = False
                
                if not all_commits_successful:
                    last_flush_time = time.time()

            # Reset timer if buffers are empty
            elif not any(event_buffers.values()) and should_flush_by_time:
                last_flush_time = current_time

    except KeyboardInterrupt:
        logging.info("--- Shutting down storage app ---")
    finally:
        consumer.close()
        db_connection.close()

if __name__ == "__main__":
    main()