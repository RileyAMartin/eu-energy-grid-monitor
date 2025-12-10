import logging
from datetime import datetime, timezone
from confluent_kafka import Producer

from eugrid_monitor_core.service import ServiceWorker
from ..api.client import EntsoeClient
from ..ingestors.price import PriceIngestor
from ..query_configs import RecentWindowQueryConfig
from ..config import settings

class PriceIngestionWorker(ServiceWorker):
    """
    Manages the ingestion for Day-Ahead Price EIC codes.
    """

    def __init__(self, client: EntsoeClient, producer: Producer):
        self._client = client
        self._producer = producer
        self._eic_codes = settings.EIC_CODES_PRICE

    def run_cycle(self) -> None:
        logging.info("--- Beginning price ingestion cycle ---")
        query_config = RecentWindowQueryConfig(hours_to_fetch=3)

        for i, eic_code in enumerate(self._eic_codes):
            logging.info(f"Fetching Prices for {eic_code} ({i+1}/{len(self._eic_codes)})")

            ingestor = PriceIngestor(
                producer=self._producer,
                eic_code=eic_code,
                client=self._client,
                query_config=query_config,
            )
            ingestor.run_ingestion_cycle()

    def shutdown(self) -> None:
        """
        The price worker's resources are managed by the orchestrator,
        so it doesn't need to anything here.
        """
        pass