import os
import pytest
from datetime import datetime, timedelta, timezone
from ingest_app.app.parsers.price import PriceParser
from eugrid_monitor_core.models import RawPriceEvent

_test_dir = os.path.dirname(os.path.abspath(__file__))
_test_data_dir = os.path.join(os.path.dirname(_test_dir), "test_data")
_PRICE_TEST_HAPPY_PATH = os.path.join(_test_data_dir, "price-test-happy.xml")

@pytest.fixture
def price_document_happy():
    with open(_PRICE_TEST_HAPPY_PATH) as f:
        return f.read()

@pytest.fixture
def parser():
    return PriceParser()

def test_parse_price_document_happy(parser, price_document_happy):
    """
    Verifies that the price parser correctly extracts EIC code, currency,
    timestamps, and price amounts from an A03 curve.
    """
    # Expected values
    eic_code = "10YAT-APG------L"
    currency = "EUR"
    start_time = datetime(2025, 12, 1, 0, 0, tzinfo=timezone.utc)
    prices = [10.25, 10.25, 20.25, 20.25]

    expected_events = []
    
    current_time = start_time
    for price in prices:
        next_time = current_time + timedelta(minutes=15)
        
        event = RawPriceEvent(
            eic_code=eic_code,
            start_time=current_time,
            end_time=next_time,
            price_amount=price,
            currency=currency
        )
        expected_events.append(event)
        current_time = next_time

    actual_events = parser.parse(price_document_happy)

    assert len(actual_events) == len(expected_events)
    for i, event in enumerate(actual_events):
        assert event == expected_events[i]