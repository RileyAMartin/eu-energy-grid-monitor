import time
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer
from .ingestors.generation import GenerationIngestor
from .query_configs import RecentWindowQueryConfig
from .api.client import EntsoeClient
from .config import settings


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    producer = Producer({
        'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': settings.KAFKA_SASL_USERNAME,
        'sasl.password': settings.KAFKA_SASL_PASSWORD,        
    })
    
    api_client = EntsoeClient(api_key=settings.ENTSOE_API_KEY)

    logging.info(f"--- Starting ingestion service ---")
    try:
        while True:
            now = datetime.now(timezone.utc)

            # At 2AM UTC, do a deep backfill of the last 72 hours
            if now.hour == settings.DEEP_BACKFILL_HOUR_UTC:
                logging.info("--- Beginning daily deep backfill (last 72 hours) ---")
                query_config = RecentWindowQueryConfig(hours_to_fetch=72)
            else:
                logging.info("--- Beginning hourly backfill (last 3 hours) ---")
                query_config = RecentWindowQueryConfig(hours_to_fetch=3)
            
            for i, eic_code in enumerate(settings.EIC_CODES_GENERATION):
                logging.info(f"Processing {eic_code} ({i+1}/{len(settings.EIC_CODES_GENERATION)})")

                ingestor = GenerationIngestor(
                    producer=producer,
                    eic_code=eic_code,
                    client=api_client,
                    query_config=query_config,
                )
                ingestor.run_ingestion_cycle()

                producer.poll(0)
                time.sleep(1)

            remaining_messages = producer.flush(timeout=10.0)
            if remaining_messages > 0:
                logging.warning(f"--- {remaining_messages} messages failed to deliver to Kafka. ---")

            # Sleep until the start of the next hour
            current_timestamp = time.time()
            seconds_until_next_hour = 3600 - (current_timestamp % 3600)

            logging.info(f"Sleeping for {seconds_until_next_hour} seconds.")
            time.sleep(seconds_until_next_hour)

    except KeyboardInterrupt:
        logging.info("--- Shutting down ingestion ---")
        producer.flush()

if __name__ == "__main__":
    main()