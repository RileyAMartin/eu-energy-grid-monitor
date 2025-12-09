from datetime import datetime
from .base import BaseIngestor
from ..parsers.pricing import PricingParser
from eugrid_monitor_core.models import RawPriceEvent
from eugrid_monitor_core.topics import RAW_PRICING_EVENTS

class PricingIngestor(BaseIngestor):
    """Ingestor for day ahead prices."""

    _DOCUMENT_TYPE = "A44"
    _CONTRACT_TYPE = "A01"  # Day-ahead

    _parser = PricingParser()

    @property
    def topic_name(self) -> str:
        return RAW_PRICING_EVENTS
    
    def _parse_response(self, response_content: str) -> list[RawPriceEvent]:
        return self._parser.parse(response_content)

    def _get_query_params(self, start_time: datetime, end_time: datetime) -> dict[str, any]:
        """
        Constructs params for day ahead prices.
        """
        return {
            "documentType": self._DOCUMENT_TYPE,
            "contract_MarketAgreement.type": self._CONTRACT_TYPE,
            "in_Domain": self._eic_code,
            "out_Domain": self._eic_code,
            "periodStart": start_time.strftime("%Y%m%d%H%M"),
            "periodEnd": end_time.strftime("%Y%m%d%H%M"),
        }
