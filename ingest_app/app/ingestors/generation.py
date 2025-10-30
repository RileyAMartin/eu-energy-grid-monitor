from datetime import datetime
from .base import BaseIngestor
from parsers.generation import parse_generation_document
from eugrid_monitor_core.models import RawGenerationEvent, KafkaTopicConfig
from eugrid_monitor_core.topic_configs import RAW_GENERATION_EVENTS

class GenerationIngestor(BaseIngestor):
    """A class to handle ingestion from the Energy Generation By Type endpoint."""

    DOCUMENT_TYPE = "A75"
    PROCESS_TYPE = "A16"
    NSMAP = {"doc": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"} # XML namespace for Market Documents

    @property
    def kafka_topic_config(self) -> KafkaTopicConfig:
        return RAW_GENERATION_EVENTS

    def _parse_response(self, response_content: str) -> list[RawGenerationEvent]:
        """Parses the XML response into a list of standardised records."""
        return parse_generation_document(response_content)

    def _build_url(self, start_time: datetime, end_time: datetime) -> str:
        """Returns a URL for the ENTSO-E API (w/o the API key, which is added by the Fetcher)"""
        start_time_str = start_time.strftime("%Y%m%d%H%M")
        end_time_str = end_time.strftime("%Y%m%d%H%M")
        return (
            f"{self._api_url}?documentType={self.DOCUMENT_TYPE}"
            f"&processType={self.PROCESS_TYPE}"
            f"&in_Domain={self._eic_code}"
            f"&periodStart={start_time_str}&periodEnd={end_time_str}"
        )
