from datetime import datetime
from base import BaseIngestor
from parsers.generation import parse_generation_document

class GenerationIngestor(BaseIngestor):
    """A class to handle ingestion from the Energy Generation By Type endpoint."""

    DOCUMENT_TYPE = "A75"
    PROCESS_TYPE = "A16"
    NSMAP = {"doc": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"} # XML namespace for Market Documents

    @property
    def topic_name(self) -> str:
        """The Kafka topic to publish events to."""
        return "raw-generation-events"

    def _parse_response(self, response_content: str) -> list[dict]:
        """Parses the XML response into a list of standardised records."""
        return parse_generation_document(response_content)

    def _build_url(self, start_time: datetime, end_time: datetime) -> str:
        """Returns a URL for the ENTSO-E API (sans the API key which must be added in an ApiFetcher.)"""
        start_time_str = start_time.strftime("%Y%m%d%H%M")
        end_time_str = end_time.strftime("%Y%m%d%H%M")
        return (
            f"{self.API_URL}?documentType={self.DOCUMENT_TYPE}"
            f"&processType={self.PROCESS_TYPE}"
            f"&in_Domain={self._eic_code}"
            f"&periodStart={start_time_str}&periodEnd={end_time_str}"
        )
