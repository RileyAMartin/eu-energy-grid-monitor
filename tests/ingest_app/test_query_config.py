import pytest
from datetime import datetime, timezone, timedelta
from freezegun import freeze_time
from ingest_app.app.query_configs import DailyAdaptableQueryConfig
from ingest_app.app.exceptions import NoDataFoundError

@pytest.fixture
def config():
    return DailyAdaptableQueryConfig()

@freeze_time("2025-01-02 10:15:00")
def test_get_time_window_good_path(config):
    """
    On a new day, the query config should return yesterday's window
    and update its internal state.
    """
    expected_start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    expected_end = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    
    start_time, end_time = config.get_time_window()

    assert (start_time, end_time) == (expected_start, expected_end)
    assert config._last_new_day_attempted == expected_start.date()

@freeze_time("2025-01-02 10:15:00")
def test_get_time_window_already_attempted(config):
    """
    If we call the same method on the same day, it should return (None, None).
    """
    # First run
    config.get_time_window()
    assert config._last_new_day_attempted == datetime(2025, 1, 1).date()

    # Second run
    start_time, end_time = config.get_time_window()
    assert (start_time, end_time) == (None, None)

@freeze_time("2025-01-02 10:15:00")
def test_get_time_window_prioritises_retry_queue(config):
    """
    Ensure that the query config prioritises times in the retry queue
    over anything else.
    """
    
    # Failed jobs that appear in the retry queue
    retry_job_1 = datetime(2024, 12, 31, 0, 0, 0, tzinfo=timezone.utc)
    retry_job_2 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    config._retry_queue = [retry_job_1, retry_job_2]

    # First round
    start_time_1, end_time_1 = config.get_time_window()
    assert start_time_1 == retry_job_1
    assert end_time_1 == retry_job_1 + timedelta(days=1)
    assert len(config._retry_queue) == 1
    assert config._last_new_day_attempted is None

    # Second round
    start_time_2, _ = config.get_time_window()
    assert start_time_2 == retry_job_2
    assert len(config._retry_queue) == 0
    
    # Final round - should return yesterday's job
    start_time_3, end_time_3 = config.get_time_window()
    expected_start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    expected_end = expected_start + timedelta(days=1)
    assert start_time_3 == expected_start
    assert end_time_3 == expected_end
    assert config._last_new_day_attempted == expected_start.date()
