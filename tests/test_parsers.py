from datetime import datetime, timedelta, timezone
from eugrid_monitor_core.models import RawGenerationEvent
from ingest_app.app.parsers.generation import parse_generation_document
import pytest

@pytest.fixture
def generation_document():
    with open("tests/test_data/generation-test.xml") as f:
        return f.read()


def test_parse_generation_document(generation_document):
    # Attributes of test events
    eic_code = "10Y1001A1001A016"
    source_document_mrid = "a823f80efb254f78afd9d247146f015b"
    measurement_unit = "MAW"
    quantity_mw_a03 = 10
    quantity_mw_a01 = 20
    psr_type_code_a01 = "B04"
    psr_type_code_a03 = "B05"
    initial_start_time = datetime(2025, 8, 20, 0, 0, tzinfo=timezone.utc) # 20/08/2025, 00:00
    start_time = initial_start_time

    test_events = []

    # Set up A01 test events
    for _ in range(4):
        end_time = start_time + timedelta(minutes=15)
        new_event = RawGenerationEvent(
            eic_code=eic_code,
            source_document_mrid=source_document_mrid,
            measurement_unit=measurement_unit,
            start_time = start_time,
            end_time = end_time,
            quantity_mw = quantity_mw_a01,
            psr_type_code=psr_type_code_a01
        )
        start_time = end_time
        test_events.append(new_event)

    # Set up A03 events
    start_time = initial_start_time
    for _ in range(4):
        end_time = start_time + timedelta(minutes=15)
        new_event = RawGenerationEvent(
            eic_code=eic_code,
            source_document_mrid=source_document_mrid,
            measurement_unit=measurement_unit,
            start_time = start_time,
            end_time = end_time,
            quantity_mw = quantity_mw_a03,
            psr_type_code=psr_type_code_a03
        )
        start_time = end_time
        test_events.append(new_event)

    events = parse_generation_document(generation_document)  # Events parsed from the XML file

    assert len(events) == len(test_events)
    for e in events:
        assert e in test_events