from datetime import datetime
from dateutil.relativedelta import relativedelta
from lxml import etree
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
    period_xml: etree.Element, shared_attributes: dict, curve_type: str = "A01"
) -> list[dict]:
    """Converts a <Period> xml element to a list of dicts with attributes "start_time", "end_time", and "quantity"""

    # Get start and end intervals
    start_str = (
        period_xml.xpath(".//doc:start", namespaces=NSMAP)[0]
        .text.replace("Z", "+00:00")
        .strip()
    )
    end_str = (
        period_xml.xpath(".//doc:end", namespaces=NSMAP)[0]
        .text.replace("Z", "+00:00")
        .strip()
    )
    start_dt = datetime.fromisoformat(start_str)
    end_dt = datetime.fromisoformat(end_str)

    # Get the duration of each Point in the Period (i.e. the resolution)
    resolution_str = period_xml.xpath(".//doc:resolution", namespaces=NSMAP)[
        0
    ].text.strip()
    resolution_delta = _parse_resolution_string(resolution_str)
    if not resolution_delta:
        return []

    # Get the Points and convert them to intervals with quantities
    points = period_xml.xpath(".//doc:Point", namespaces=NSMAP)
    events = []
    if curve_type == "A01":
        curr_dt = start_dt
        next_dt = curr_dt + resolution_delta
        for point in points:
            quantity = float(
                point.xpath(".//doc:quantity", namespaces=NSMAP)[0].text.strip()
            )
            if quantity == None:
                quantity = 0
            interval = {
                "start_time": curr_dt.isoformat(),
                "end_time": next_dt.isoformat(),
                "quantity_mw": quantity,
                **shared_attributes,
            }
            events.append(interval)
            curr_dt = next_dt
            next_dt += resolution_delta

    elif curve_type == "A03":

        # The "A03" curve type uses a segmented Point representation to save space
        # Each Point in the Period can represent more than 1 interval
        num_positions = int((end_dt - start_dt) / resolution_delta)
        num_points = len(points)
        curr_dt = start_dt
        next_dt = curr_dt + resolution_delta
        for i, point in enumerate(points):
            curr_pos = int(
                point.xpath(".//doc:position", namespaces=NSMAP)[0].text.strip()
            )
            quantity = float(
                point.xpath(".//doc:quantity", namespaces=NSMAP[0].text.strip())
            )
            if quantity == None:
                quantity = 0

            # If you're at the final position, interpolate all remaining Points until the end of the Period's interval
            # Otherwise, interpolate up to the next Point
            if i + 1 == num_points:
                next_pos = num_positions
            else:
                next_pos = int(
                    point.xpath(".//doc:position", namespaces=NSMAP)[0].text.strip()
                )

            for _ in range(curr_pos, next_pos):
                interval = {
                    "start_time": curr_dt,
                    "end_time": next_dt,
                    "quantity": quantity,
                    **shared_attributes,
                }
                events.append(interval)
                curr_dt = next_dt
                next_dt += resolution_delta
    else:
        return None

    return events


def parse_generation_document(xml_content: str) -> list[dict]:
    """
    Parses a full Generation Load Document XML and returns a single,
    flat list of all individual generation events.
    """
    try:
        doc_xml = etree.fromstring(xml_content)
    except etree.XMLSyntaxError as e:
        logging.error(f"Invalid XML content: {e}")
        return []

    all_events = []

    # Extract document-level info
    document_mrid = doc_xml.xpath("/doc:GL_MarketDocument/doc:mRID", namespaces=NSMAP)[
        0
    ].text.strip()
    timeseries_list = doc_xml.xpath(".//doc:TimeSeries", namespaces=NSMAP)

    for ts_xml in timeseries_list:
        # Since we're only looking for energy generation, we only need to parse
        # TimeSeries from inBiddingZones
        in_domain_elements = ts_xml.xpath(
            ".//doc:inBiddingZone_Domain.mRID", namespaces=NSMAP
        )
        if not in_domain_elements:
            continue

        try:
            # Get shared attributes for this TimeSeries
            shared_attributes = {
                "eic_code": in_domain_elements[0].text.strip(),
                "psr_type_code": ts_xml.xpath(".//doc:psrType", namespaces=NSMAP)[
                    0
                ].text.strip(),
                "measurement_unit": ts_xml.xpath(
                    ".//doc:quantity_Measure_Unit.name", namespaces=NSMAP
                )[0].text.strip(),
                "source_document_mrid": document_mrid,
            }

            # Get the period and parse it into a list of events
            period_xml = ts_xml.xpath(".//doc:Period", namespaces=NSMAP)[0]
            period_curve_type = ts_xml.xpath(".//doc:curveType", namespaces=NSMAP)[0].text.strip()

            events_from_period = _parse_period_to_events(
                period_xml, shared_attributes, period_curve_type
            )
            all_events.extend(events_from_period)

        except IndexError as e:
            logging.warning(f"Skipping a TimeSeries due to missing required tags: {e}")
            continue

    return all_events
