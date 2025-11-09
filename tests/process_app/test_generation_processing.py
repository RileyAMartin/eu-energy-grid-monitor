import pytest
from datetime import datetime, timedelta, timezone
from process_app.app.processors.generation import process_generation_event
from process_app.app.config import settings
from process_app.app.exceptions import InvalidEicCodeError, InvalidPsrTypeCodeError, InvalidEventDurationError
from eugrid_monitor_core.models import RawGenerationEvent, EnrichedGenerationEvent

def test_process_generation_event_happy():
    """
    Ensure that the raw event is processed correctly
    and that the enriched event has the correct values.
    """
    # EIC details (GB-NI)
    eic_code = "10Y1001A1001A016"
    eic_display_name = "GB-NI"
    eic_long_name = "Northern Ireland"
    countries = ["gb-nie"]
    bidding_zone = "NIE"

    # PSR Details (B14 - Nuclear)
    psr_type_code = "B14"
    psr_type_name = "Nuclear"
    psr_kg_co2e_mwh = 13

    # Event specific details
    start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)  # 01/01/2025 00:00
    end_time = datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)  # 01/01/2025 01:00
    measurement_unit = "MAW"
    source_document_mrid = "MRID-TEST"
    quantity_mw = 160

    raw_test_event = RawGenerationEvent(
        eic_code=eic_code,
        start_time=start_time,
        end_time=end_time,
        quantity_mw=quantity_mw,
        measurement_unit=measurement_unit,
        psr_type_code=psr_type_code,
        source_document_mrid=source_document_mrid        
    )

    enriched_events = process_generation_event(raw_test_event, settings.PSR_TYPE_MAPPINGS, settings.EIC_MAPPINGS)
    
    # Expected calculated values for the enriched event
    # The raw event (of duration 1 hour) is split into 4 events (of duration 15 minutes)
    expected_kg_co2e = psr_kg_co2e_mwh * quantity_mw / 4
    times = [start_time]
    times.extend([start_time + timedelta(minutes=15*i) for i in range(1, 5)])

    assert len(enriched_events) == 4
    for i, event in enumerate(enriched_events):

        # EIC info
        assert event.eic_code == eic_code
        assert event.eic_display_name == eic_display_name
        assert event.eic_long_name ==  eic_long_name
        assert event.bidding_zone == bidding_zone
        assert set(event.countries) == set(countries)
        
        # PSR Info
        assert event.psr_type_code == psr_type_code
        assert event.psr_type_name == psr_type_name
        assert event.carbon_output_kg_co2e == expected_kg_co2e

        # Check the event intervals
        expected_start = times[i]
        expected_end = times[i+1]
        assert event.start_time == expected_start
        assert event.end_time == expected_end

def test_process_generation_event_invalid_psr():
    """The processing function should throw an error where there's an invalid PSR type."""
    invalid_event = RawGenerationEvent(
        eic_code = "10Y1001A1001A016",
        start_time=datetime(2025, 1, 1, 0, 0, 0),
        end_time=datetime(2025, 1, 1, 1, 0, 0),
        psr_type_code="INVALID-PSR-TYPE",
        quantity_mw=100,
        measurement_unit="MAW",
        source_document_mrid="TEST-MRID"
    )
    with pytest.raises(InvalidPsrTypeCodeError):
        process_generation_event(invalid_event, settings.PSR_TYPE_MAPPINGS, settings.EIC_MAPPINGS)

def test_process_generation_event_invalid_eic():
    """The processing function should throw an error where there's an invalid EIC code."""
    invalid_event = RawGenerationEvent(
        eic_code = "INVALID-EIC",
        start_time=datetime(2025, 1, 1, 0, 0, 0),
        end_time=datetime(2025, 1, 1, 1, 0, 0),
        psr_type_code="B14",
        quantity_mw=100,
        measurement_unit="MAW",
        source_document_mrid="TEST-MRID"
    )
    with pytest.raises(InvalidEicCodeError):
        process_generation_event(invalid_event, settings.PSR_TYPE_MAPPINGS, settings.EIC_MAPPINGS)
