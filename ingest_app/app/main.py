import logging
from confluent_kafka import Producer
from .workers.generation import GenerationIngestionWorker
from .workers.price import PriceIngestionWorker
from .workers.orchestrator import IngestionOrchestrator
from eugrid_monitor_core.service import ServiceRunner
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
    
    generation_worker = GenerationIngestionWorker(api_client, producer)
    price_worker = PriceIngestionWorker(api_client, producer)

    orchestrator = IngestionOrchestrator(
        workers=[generation_worker, price_worker],
        producer=producer
    )

    # Ingestion is orchestrated every hour
    runner = ServiceRunner(worker=orchestrator, sleep_interval=3600)
    runner.run()

if __name__ == "__main__":
    main()