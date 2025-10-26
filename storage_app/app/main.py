import json
import sys
import time
import psycopg2
import psycopg2.extras
import logging
from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError
from typing import List
from config import settings
from eugrid_monitor_core.models import EnrichedGenerationEvent

def perform_bulk_insert(conn, table_name: str, columns: List[str], conflict_columns: List[str], events: List[dict]):

    if not events:
        return 0

    data_tuples = [
        tuple(event.get(col) for col in columns)
        for event in events
    ]

    if not conflict_columns:
        insert_query = f"""
            INSERT INTO {table_name} ({", ".join(f'"{c}"' for c in columns)})
            VALUES %s;
        """

    else:
        insert_query = f"""
            INSERT INTO {table_name} ({", ".join(f'"{c}"' for c in columns)})
            VALUES %s
            ON CONFLICT ({", ".join(f'"{c}"' for c in conflict_columns)})
            DO NOTHING;
        """

    cursor = None
    try:
        cursor = conn.cursor()

        # Perform the bulk insert
        psycopg2.extras.execute_values(
            cursor,
            insert_query,
            data_tuples,
            template=None,
            page_size=100
        )
        conn.commit()

        inserted_count = cursor.rowcount
        logging.info(f"--- Attempted insert of {inserted_count} rows into {table_name} ---")
        return inserted_count
    
    except Exception as e:
        logging.error(f"Database bulk insert failed for {table_name}: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

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
    consumer_config = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": settings.KAFKA_SASL_USERNAME,
        "sasl.password": settings.KAFKA_SASL_PASSWORD,
        "group.id": settings.KAFKA_GROUP_ID,
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest"
    }
    consumer = Consumer(consumer_config)
    topics = list(settings.DB_MAPPINGS.keys())

    # Main loop
    event_buffers = {topic: [] for topic in topics}  # Each topic will be uploaded to the db individually
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

                model = settings.DB_MAPPINGS[topic]["model"]  # Pydantic model for this event type
                event = model.model_validate(raw_message)
                event_buffers[topic].append(event.model_dump(mode="json"))

            except (json.JSONDecodeError, ValidationError) as e:
                logging.error(f"Failed to process message: {e}")
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