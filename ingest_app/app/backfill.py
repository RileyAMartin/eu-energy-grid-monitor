import logging
import time
from confluent_kafka import Producer
from .config import settings
from .ingestors.generation import GenerationIngestor
from .api.client import EntsoeApiFetcher
from .query_configs import YearlyBackfillQueryConfig

def backfill():
    """Ingests all possible data from the period between today and one year prior."""

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    producer = Producer({
        'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': settings.KAFKA_SASL_USERNAME,
        'sasl.password': settings.KAFKA_SASL_PASSWORD,        
    })
    fetcher = EntsoeApiFetcher(settings.ENTSOE_API_KEY)

    # Initialise the ingestion tasks
    tasks = []
    for eic_code in settings.EIC_CODES_GENERATION:
        query_config = YearlyBackfillQueryConfig(eic_code)
        ingestor = GenerationIngestor(
            producer,
            eic_code,
            fetcher,
            query_config,
            settings.ENTSOE_API_URL
        )
        tasks.append(ingestor)

    # Ingestion loop
    try:
        while True:
            logging.info("--- Starting new daily ingestion cycle ---")

            for task in tasks:
                task.run_ingestion_cycle()
                time.sleep(3)
            
            remaining_messages = producer.flush()
            if remaining_messages > 0:
                logging.warning(f"--- {remaining_messages} messages failed to deliver to Kafka. ---")
            else:
                logging.info("--- All messages delivered successfully to Kafka. ---")

    except KeyboardInterrupt as e:
        logging.info("--- Shutting down ingestion ---")
        producer.flush()

if __name__ == "__main__":
    backfill()