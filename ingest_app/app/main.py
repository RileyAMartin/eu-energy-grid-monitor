from confluent_kafka import Producer
from ingestors.generation import GenerationIngestor
from ingestors.query_configs import DailyQueryConfig
from api.client import EntsoeApiFetcher
from config import settings
import logging
import time



def main():
    """
    Sets up the environment and runs the ingestion cycle for the ENTSOE-API.
    For now only energy generation metrics are ingested, but more will be added.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Shared instances across each ingestor
    producer = Producer({
        'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': settings.KAFKA_SASL_USERNAME,
        'sasl.password': settings.KAFKA_SASL_PASSWORD,        
    })
    fetcher = EntsoeApiFetcher(settings.ENTSOE_API_KEY)
    query_config = DailyQueryConfig()
    
    # Initialise the ingestion tasks
    tasks = []
    for eic_code in settings.EIC_CODES_GENERATION:
        ingestor = GenerationIngestor(
            producer,
            eic_code,
            fetcher,
            query_config,
            settings.API_URL
        )
        tasks.append(ingestor)

    # Recurring ingestion loop
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

        logging.info("--- Daily cycle complete. Sleeping for 24 hours ---")
        time.sleep(86400) # 1 day - TODO: implement adaptive sleeping and then cron jobs


if __name__ == "__main__":
    main()