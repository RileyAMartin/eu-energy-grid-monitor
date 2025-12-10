from .base import BaseEntsoeParser
from eugrid_monitor_core.models import RawPriceEvent

class PriceParser(BaseEntsoeParser):
    """
    Parser for price documents from the ENTSO-E API.
    """

    def _extract_timeseries_context(self, ts_node, nsmap) -> dict:
        """
        Extracts metadata shared by all Points in this TimeSeries.
        """
        domain_node = ts_node.xpath(".//doc:in_Domain.mRID", namespaces=nsmap)
        if not domain_node:
            return
        
        # Extract currency
        currency_node = ts_node.xpath(".//doc:currency_Unit.name", namespaces=nsmap)
        currency = currency_node[0].text.strip() if currency_node else "EUR"

        return {
            "eic_code": domain_node[0].text.strip(),
            "currency": currency
        }

    def _extract_point_data(self, point_node, nsmap) -> dict:
        """
        Extracts the price amount from the Point.
        """
        price_node = point_node.xpath(".//doc:price.amount", namespaces=nsmap)
        return {
            "price_amount": float(price_node[0].text) if price_node else 0.0
        }

    def _create_event(self, data: dict) -> RawPriceEvent:
        """Validates the dictionary into a Pydantic model."""
        return RawPriceEvent.model_validate(data)
