import time
import logging
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from confluent_kafka import Producer
from .ingestors.generation import GenerationIngestor
from .query_configs import RollingBackfillQueryConfig
from .api.client import EntsoeApiFetcher
from .config import settings

def _get_next_run_timestamp(target_hour_utc: int) -> float:
    """
    Calculates the timestamp for the next cycle based on the target hour (1 AM UTC).
    """
    now_utc = datetime.now(timezone.utc)
    next_run_utc = now_utc.replace(hour=target_hour_utc, minute=0, second=0, microsecond=0)

    # If we're already past 1 AM UTC today, the next run is tomorrow
    if now_utc >= next_run_utc:
        next_run_utc += relativedelta(days=1)

    return next_run_utc.timestamp()


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    producer = Producer({
        'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': settings.KAFKA_SASL_USERNAME,
        'sasl.password': settings.KAFKA_SASL_PASSWORD,        
    })
    fetcher = EntsoeApiFetcher(settings.ENTSOE_API_KEY)

    tasks = []
    
    for eic_code in settings.EIC_CODES_GENERATION:
        backfill_config = RollingBackfillQueryConfig(eic_code, days_to_backfill=3)
        tasks.append(GenerationIngestor(
            producer, eic_code, fetcher, backfill_config, settings.ENTSOE_API_URL
        ))

    INGESTION_INTERVAL_SECONDS = 86400  # 24 hours
    TARGET_HOUR_UTC = 1  # 1:00 AM UTC

    # Calculate the first run time
    next_run_timestamp = _get_next_run_timestamp(TARGET_HOUR_UTC)
    
    logging.info(f"--- Starting daily ingestion service ---")
    logging.info(f"Next run scheduled for: {datetime.fromtimestamp(next_run_timestamp, tz=timezone.utc)}")

    try:
        while True:
            current_time = time.time()

            # Check timer to see if we can run
            if current_time >= next_run_timestamp:
                logging.info("--- Starting new DAILY ingestion cycle ---")
                for task in tasks:
                    task.run_ingestion_cycle()
                    time.sleep(3)
                
                logging.info("--- DAILY cycle complete ---")
                
                next_run_timestamp += INGESTION_INTERVAL_SECONDS
                logging.info(f"Next run scheduled for {datetime.fromtimestamp(next_run_timestamp, tz=timezone.utc)}")

                remaining_messages = producer.flush(timeout=10.0)
                if remaining_messages > 0:
                    logging.warning(f"--- {remaining_messages} messages failed to deliver to Kafka. ---")

            # Sleep until the next run or 5 minutes, whichever is shorter
            sleep_duration = max(0, min(next_run_timestamp - time.time(), 300))
            time.sleep(sleep_duration)

    except KeyboardInterrupt:
        logging.info("--- Shutting down ingestion ---")
        producer.flush()

if __name__ == "__main__":
    main()