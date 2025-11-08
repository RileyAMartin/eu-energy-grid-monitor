import pytest
import os
from datetime import datetime, timedelta, timezone
from eugrid_monitor_core.models import RawGenerationEvent
from ingest_app.app.parsers.generation import parse_generation_document

_test_dir = os.path.dirname(os.path.abspath(__file__))
_test_data_dir = os.path.join(os.path.dirname(_test_dir), "test_data")
_GENERATION_TEST_HAPPY_FILE_PATH = os.path.join(_test_data_dir, "generation-test-happy.xml")
_GENERATION_TEST_MISSING_MRID_FILE_PATH = os.path.join(_test_data_dir, "generation-test-missing-mrid.xml")
_GENERATION_TEST_MISSING_TIMESERIES_FILE_PATH = os.path.join(_test_data_dir, "generation-test-missing-timeseries.xml")


@pytest.fixture
def generation_document_happy():
    with open(_GENERATION_TEST_HAPPY_FILE_PATH) as f:
        return f.read()
    
@pytest.fixture
def generation_document_missing_mrid():
    with open(_GENERATION_TEST_MISSING_MRID_FILE_PATH) as f:
        return f.read()

@pytest.fixture
def generation_document_missing_timeseries():
    with open(_GENERATION_TEST_MISSING_TIMESERIES_FILE_PATH) as f:
        return f.read()

def test_parse_generation_document_happy(generation_document_happy):
    """All events should be returned with the correct attributes."""
    # Attributes of test events
    eic_code = "10Y1001A1001A016"
    source_document_mrid = "a823f80efb254f78afd9d247146f015b"
    measurement_unit = "MAW"
    quantity_mw_a03 = 10
    quantity_mw_a01 = 20
    psr_type_code_a01 = "B04"
    psr_type_code_a03 = "B05"
    initial_start_time = datetime(2025, 8, 20, 0, 0, tzinfo=timezone.utc) # 20/08/2025 00:00
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

    events = parse_generation_document(generation_document_happy)

    assert len(events) == len(test_events)
    for e in events:
        assert e in test_events

def test_parse_generation_document_missing_mrid(generation_document_missing_mrid):
    """An IndexError should be thrown on a missing MRID."""
    with pytest.raises(IndexError):
        parse_generation_document(generation_document_missing_mrid)

def test_parse_generation_document_missing_timeseries(generation_document_missing_timeseries):
    """An empty list should be returned on a missing timeseries."""
    assert parse_generation_document(generation_document_missing_timeseries) == []
