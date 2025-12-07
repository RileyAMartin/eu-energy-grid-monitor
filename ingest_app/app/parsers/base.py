import logging
from abc import ABC, abstractmethod
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict
from lxml import etree
from eugrid_monitor_core.models import EntsoeEvent

class BaseEntsoeParser(ABC):
    """
    Base strategy for parsing XML documents from the ENTSO-E API.
    """

    def parse(self, xml_content: bytes) -> List[EntsoeEvent]:
        """
        Parses the XML content from an ENTSO-E API response.
        Returns a list of EntsoeEvent models.
        """
        if not xml_content:
            return []
        
        try:
            doc = etree.fromstring(xml_content)
        except etree.XMLSyntaxError as e:
            logging.error(f"Error parsing XML: {e}")
            return []
        
        nsmap = self._get_namespaces(doc)
        all_events = []

        # Extract the info from every TimeSeries in the XML 
        for ts_node in doc.xpath(".//doc:TimeSeries", namespaces=nsmap):
            try:
                # Get context shared by this TimeSeries
                context = self._extract_timeseries_context(ts_node, nsmap)
                if not context:
                    continue

                curve_type_node = ts_node.xpath(".//doc:curveType", namespaces=nsmap)
                curve_type = curve_type_node[0].text.strip() if curve_type_node else "A01"

                # Process each period 
                for period_node in ts_node.xpath(".//doc:Period", namespaces=nsmap):
                    events = self._parse_period(period_node, context, curve_type, nsmap)
                    all_events.extend(events)

            except Exception as e:
                logging.warning(f"Error parsing TimeSeries: {e}", exc_info=True)
                continue

        return all_events

    def _parse_period(self, period_node, context: dict, curve_type: str, nsmap: dict) -> list[EntsoeEvent]:
        """
        Parses a <Period> block into individual events, handling A01 vs A03 logic.
        """
        # Get Start Time
        start_str = period_node.xpath(".//doc:timeInterval/doc:start", namespaces=nsmap)[0].text
        period_start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))

        # Get Resolution
        res_str = period_node.xpath(".//doc:resolution", namespaces=nsmap)[0].text
        resolution_delta = self._parse_resolution(res_str)
        if not resolution_delta:
            return []

        points = period_node.xpath(".//doc:Point", namespaces=nsmap)
        events = []

        if curve_type == "A01":
            # A01 is a standard curve type which includes 1 point per 1 interval ub the XML.
            curr_dt = period_start_dt
            next_dt = curr_dt + resolution_delta

            for point in points:
                # Get the desired value for this Point
                point_data = self._extract_point_data(point, nsmap)
                
                if point_data:
                    full_data = {
                        **context, 
                        **point_data, 
                        "start_time": curr_dt, 
                        "end_time": next_dt
                    }
                    event = self._create_event(full_data)
                    events.append(event)
                
                curr_dt = next_dt
                next_dt += resolution_delta

        elif curve_type == "A03":
            # A03 curve type is interpolated and allows for multiple intervals to be
            # condensed into a single Period in the XML.
            
            # We need the End Time to calculate total positions for the final segment
            end_str = period_node.xpath(".//doc:timeInterval/doc:end", namespaces=nsmap)[0].text
            period_end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            
            # Convert relativedelta to timedelta for division
            resolution_timedelta = (period_start_dt + resolution_delta) - period_start_dt
            if resolution_timedelta.total_seconds() == 0:
                 raise ValueError("Resolution results in zero timedelta for division")

            total_period_duration = period_end_dt - period_start_dt
            num_positions = int(total_period_duration / resolution_timedelta)
            num_points = len(points)
            
            for i, point in enumerate(points):
                curr_pos = int(point.xpath(".//doc:position", namespaces=nsmap)[0].text)
                
                # Determine where this segment ends (either next point's position or end of period)
                if i + 1 == num_points:
                    next_pos = num_positions + 1
                else:
                    next_point = points[i+1]
                    next_pos = int(next_point.xpath(".//doc:position", namespaces=nsmap)[0].text)

                # Get the value for this Point
                point_data = self._extract_point_data(point, nsmap)
                if not point_data:
                    continue

                # Calculate specific timestamps for this segment (position starts at 1)
                segment_start_dt = period_start_dt + (resolution_delta * (curr_pos - 1))

                curr_dt = segment_start_dt
                next_dt = curr_dt + resolution_delta

                # Create an event for every interval between this point and the next
                for _ in range(curr_pos, next_pos):
                    full_data = {
                        **context,
                        **point_data,
                        "start_time": curr_dt,
                        "end_time": next_dt
                    }
                    event = self._create_event(full_data)
                    events.append(event)

                    curr_dt = next_dt
                    next_dt += resolution_delta

        return events

    @abstractmethod
    def _extract_timeseries_context(self, ts_node, nsmap) -> Dict:
        """Extract metadata common to the whole TimeSeries (e.g. EIC Code)."""
        pass

    @abstractmethod
    def _extract_point_data(self, point_node, nsmap: Dict) -> Dict:
        """Extract the specific value from a point (e.g. quantity)."""
        pass

    @abstractmethod
    def _create_event(self, data: Dict) -> EntsoeEvent:
        """Returns a validated EntsoeEvent model."""
        pass

    def _get_namespaces(self, doc) -> Dict:
        try:
            ns = etree.QName(doc).namespace
            # If no namespace is found, then use the most common namespace
            if ns:
                return {"doc": ns}
            else:
                {"doc": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
        except ValueError:
            return {"doc": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}

    def _parse_resolution(self, res_str: str) -> relativedelta:
        mapping = {
            "PT15M": relativedelta(minutes=15),
            "PT30M": relativedelta(minutes=30),
            "PT60M": relativedelta(minutes=60),
            "P1D": relativedelta(days=1),
            "P7D": relativedelta(days=7),
            "P1M": relativedelta(months=1),
            "P1Y": relativedelta(years=1)
        }
        return mapping.get(res_str)
