from typing import List, Dict
from .base import BaseEntsoeParser
from eugrid_monitor_core.models import RawGenerationEvent

class GenerationParser(BaseEntsoeParser):
    """
    Parser for Generation Load Documents (GL_MarketDocument).
    """

    def _extract_timeseries_context(self, ts_node, nsmap) -> Dict:
        """
        Extracts metadata shared by all points in this TimeSeries.
        Returns None if the TimeSeries belongs to an 'outBiddingZone' (i.e. export)
        """
        domain = ts_node.xpath(".//doc:inBiddingZone_Domain.mRID", namespaces=nsmap)
        if not domain:
            return None 

        return {
            "eic_code": domain[0].text.strip(),
            "psr_type_code": ts_node.xpath(".//doc:psrType", namespaces=nsmap)[0].text.strip(),
            "measurement_unit": ts_node.xpath(".//doc:quantity_Measure_Unit.name", namespaces=nsmap)[0].text.strip(),
        }

    def _extract_point_data(self, point_node, nsmap) -> Dict:
        """
        Extracts the value for a specific Point.
        Returns a Dict containing the Quantity value.
        """
        qty_node = point_node.xpath(".//doc:quantity", namespaces=nsmap)
        return {
            "quantity_mw": float(qty_node[0].text) if qty_node else 0.0
        }

    def _create_event(self, data: dict) -> RawGenerationEvent:
        """
        Validates the given data and returns a corresponding RawGenerationEvent.
        """
        return RawGenerationEvent.model_validate(data)