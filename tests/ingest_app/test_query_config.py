import pytest
from datetime import datetime, timezone, timedelta
from freezegun import freeze_time
from ingest_app.app.query_configs import RecentWindowQueryConfig
from ingest_app.app.exceptions import NoDataFoundError

@pytest.fixture
def config_default():
    return RecentWindowQueryConfig()

@pytest.fixture
def config_custom():
    return RecentWindowQueryConfig(hours_to_fetch=24)

@freeze_time("2025-01-01 10:15:00")
def test_get_time_window_default(config_default):
    """
    The default config should return a window ending at the start of the current hour (10:00)
    and starting 3 hours prior (07:00).
    """
    expected_end = datetime(2025, 1, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
    expected_start = expected_end - timedelta(hours=3)

    start_time, end_time = config_default.get_time_window()

    assert (start_time, end_time) == (expected_start, expected_end)

@freeze_time("2025-01-01 10:15:00")
def test_get_time_window_custom(config_custom):
    """
    The custom config (24-hour window) should return a window ending with the current hour (01/01 10:00)
    and starting 1 day prior (31/12 10:00).
    """
    expected_end = datetime(2025, 1, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
    expected_start = expected_end - timedelta(hours=24)

    start_time, end_time = config_custom.get_time_window()
    
    assert (start_time, end_time) == (expected_start, expected_end)
