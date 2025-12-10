import logging
from datetime import datetime, timezone
from confluent_kafka import Producer

from eugrid_monitor_core.service import ServiceWorker
from ..api.client import EntsoeClient
from ..ingestors.generation import GenerationIngestor
from ..query_configs import RecentWindowQueryConfig
from ..config import settings

class GenerationIngestionWorker(ServiceWorker):
    """
    Manages the ingestion for all generation EIC codes.
    """

    def __init__(self, client: EntsoeClient, producer: Producer):
        self._client = client
        self._producer = producer
        self._eic_codes = settings.EIC_CODES_GENERATION
        self._deep_backfill_hour = settings.DEEP_BACKFILL_HOUR_UTC
    
    def run_cycle(self) -> None:
        now = datetime.now(timezone.utc)

        if now.hour == self._deep_backfill_hour:
            logging.info(f"--- Beginning generation deep backfill (72 hours) ---")
            query_config = RecentWindowQueryConfig(hours_to_fetch=72)
        else:
            logging.info(f"--- Beginning generation hourly backfill (3 hours) ---")
            query_config = RecentWindowQueryConfig(hours_to_fetch=3)
        
        for i, eic_code in enumerate(settings.EIC_CODES_GENERATION):
            logging.info(f"Processing {eic_code} ({i+1}/{len(settings.EIC_CODES_GENERATION)})")

            ingestor = GenerationIngestor(
                producer=self._producer,
                eic_code=eic_code,
                client=self._client,
                query_config=query_config,
            )
            ingestor.run_ingestion_cycle()

    def shutdown(self) -> None:
        """
        The generation worker's resources are managed by the orchestrator,
        so it doesn't need to anything here.
        """
        pass