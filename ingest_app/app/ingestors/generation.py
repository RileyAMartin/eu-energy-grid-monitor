from datetime import datetime
from .base import BaseIngestor
from ..parsers.generation import GenerationParser
from eugrid_monitor_core.models import RawGenerationEvent
from eugrid_monitor_core.topics import RAW_GENERATION_EVENTS

class GenerationIngestor(BaseIngestor):
    """A class to handle ingestion from the Energy Generation By Type endpoint."""

    _DOCUMENT_TYPE = "A75"
    _PROCESS_TYPE = "A16"
    _NSMAP = {"doc": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"} # XML namespace for Market Documents

    _parser = GenerationParser()

    @property
    def topic_name(self) -> str:
        return RAW_GENERATION_EVENTS

    def _parse_response(self, response_content: str) -> list[RawGenerationEvent]:
        """Parses the XML response into a list of standardised records."""
        return self._parser.parse(response_content)

    def _get_query_params(self, start_time: datetime, end_time: datetime) -> dict[str, any]:
        """
        Returns the params for fetching generation data.
        """
        return {
            "documentType": self._DOCUMENT_TYPE,
            "processType": self._PROCESS_TYPE,
            "in_domain": self._eic_code,
            "periodStart": start_time.strftime("%Y%m%d%H%M"),
            "periodEnd": end_time.strftime("%Y%m%d%H%M"),
        }
