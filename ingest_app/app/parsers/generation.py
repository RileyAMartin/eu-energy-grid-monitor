from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from lxml import etree
from eugrid_monitor_core.models import RawGenerationEvent
import logging

# XML namespace for Market Documents
NSMAP = {"doc": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}

def _parse_resolution_string(resolution: str) -> relativedelta:
    """Converts a Resolution value into a corresponding relativedelta value."""
    match resolution:
        case "PT15M":
            return relativedelta(minutes=15)
        case "PT30M":
            return relativedelta(minutes=30)
        case "PT60M":
            return relativedelta(minutes=60)
        case "P1D":
            return relativedelta(days=1)
        case "P7D":
            return relativedelta(days=7)
        case "P1M":
            return relativedelta(months=1)
        case "P1Y":
            return relativedelta(years=1)
        case _:
            logging.warning(f"Unknown resolution string found: {resolution}")
            return None


def _parse_period_to_events(
    period_xml: etree.Element, shared_attributes: dict, curve_type: str, nsmap: dict
) -> list[dict]:
    """Converts a <Period> xml element to a list of dicts with attributes "start_time", "end_time", and "quantity"""

    # Get start and end intervals
    start_str = (
        period_xml.xpath(".//doc:start", namespaces=nsmap)[0]
        .text.replace("Z", "+00:00")
        .strip()
    )
    start_dt = datetime.fromisoformat(start_str)

    # Get the duration of each Point in the Period (i.e. the resolution)
    resolution_str = period_xml.xpath(".//doc:resolution", namespaces=nsmap)[
        0
    ].text.strip()
    resolution_delta = _parse_resolution_string(resolution_str)
    
    if not resolution_delta:
        return []

    # Get the Points and convert them to intervals with quantities
    points = period_xml.xpath(".//doc:Point", namespaces=nsmap)
    events = []

    if curve_type == "A01":
        # A01: Sequential fixed-size blocks. 
        curr_dt = start_dt
        next_dt = curr_dt + resolution_delta
        
        for point in points:
            quantity_text = point.xpath(".//doc:quantity", namespaces=nsmap)[0].text
            quantity = float(quantity_text.strip()) if quantity_text else 0.0
            
            interval = {
                "start_time": curr_dt.isoformat(),
                "end_time": next_dt.isoformat(),
                "quantity_mw": quantity,
                **shared_attributes,
            }
            event = RawGenerationEvent.model_validate(interval)
            events.append(event)
            
            curr_dt = next_dt
            next_dt += resolution_delta

    elif curve_type == "A03":
        # A03: CurveType - Points define steps.
        # The value at Point P is valid from Position P up to Position P+1 (or end of period).
        
        # Calculate total positions to handle the final segment correctly
        end_str = (
            period_xml.xpath(".//doc:end", namespaces=nsmap)[0]
            .text.replace("Z", "+00:00")
            .strip()
        )
        end_dt = datetime.fromisoformat(end_str)
        
        # We need a timedelta for division, relativedelta doesn't support it directly
        resolution_timedelta = end_dt - (end_dt - resolution_delta) 
        if resolution_timedelta.total_seconds() == 0:
             raise ValueError("Resolution results in zero timedelta for division")

        total_period_duration = end_dt - start_dt
        num_positions = int(total_period_duration / resolution_timedelta)
        num_points = len(points)
        
        for i, point in enumerate(points):
            curr_pos = int(
                point.xpath(".//doc:position", namespaces=nsmap)[0].text.strip()
            )
            quantity_text = point.xpath(".//doc:quantity", namespaces=nsmap)[0].text
            quantity = float(quantity_text.strip()) if quantity_text else 0.0

            # Determine the end position for this interval
            if i + 1 == num_points:
                next_pos = num_positions + 1
            else:
                next_point = points[i+1]
                next_pos = int(
                    next_point.xpath(".//doc:position", namespaces=nsmap)[0].text.strip()
                )

            # Explicitly calculate the time this segment starts at based on position
            segment_start_dt = start_dt + ((curr_pos - 1) * resolution_delta)
            
            curr_dt = segment_start_dt
            next_dt = curr_dt + resolution_delta

            # Generate an event for every resolution step between curr_pos and next_pos
            for _ in range(curr_pos, next_pos):
                interval = {
                    "start_time": curr_dt.isoformat(),
                    "end_time": next_dt.isoformat(),
                    "quantity_mw": quantity,
                    **shared_attributes,
                }
                event = RawGenerationEvent.model_validate(interval)
                events.append(event)
                curr_dt = next_dt
                next_dt += resolution_delta

    return events


def parse_generation_document(xml_str: str) -> list[dict]:
    """
    Parses a full Generation Load Document XML and returns a single,
    flat list of all individual generation events.
    """
    if not xml_str:
        return []
        
    try:
        doc_xml = etree.fromstring(xml_str)
    except etree.XMLSyntaxError as e:
        logging.error(f"Invalid XML content: {e}")
        return []

    # Dynamic Namespace Detection
    try:
        namespace_uri = etree.QName(doc_xml).namespace
    except ValueError:
        namespace_uri = None

    if namespace_uri:
        nsmap = {"doc": namespace_uri}
    else:
        # Fallback to the known namespace if detection fails
        nsmap = {"doc": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}

    all_events = []

    try:
        timeseries_list = doc_xml.xpath(".//doc:TimeSeries", namespaces=nsmap)

        for ts_xml in timeseries_list:
            # Filter for inBiddingZone_Domain
            in_domain_elements = ts_xml.xpath(
                ".//doc:inBiddingZone_Domain.mRID", namespaces=nsmap
            )
            if not in_domain_elements:
                continue

            try:
                # Get shared attributes for this TimeSeries
                shared_attributes = {
                    "eic_code": in_domain_elements[0].text.strip(),
                    "psr_type_code": ts_xml.xpath(".//doc:psrType", namespaces=nsmap)[
                        0
                    ].text.strip(),
                    "measurement_unit": ts_xml.xpath(
                        ".//doc:quantity_Measure_Unit.name", namespaces=nsmap
                    )[0].text.strip()
                }

                # Get the curve type for the whole TimeSeries
                period_curve_type = ts_xml.xpath(".//doc:curveType", namespaces=nsmap)[0].text.strip()

                # Iterate over ALL Period elements in the TimeSeries
                periods_xml = ts_xml.xpath(".//doc:Period", namespaces=nsmap)
                
                for period_xml in periods_xml:
                    events_from_period = _parse_period_to_events(
                        period_xml, shared_attributes, period_curve_type, nsmap
                    )
                    all_events.extend(events_from_period)

            except IndexError as e:
                logging.warning(f"Skipping a TimeSeries due to missing required tags: {e}")
                continue
                
    except Exception as e:
        logging.error(f"Unexpected error during parsing: {e}")
        return []

    return all_events
