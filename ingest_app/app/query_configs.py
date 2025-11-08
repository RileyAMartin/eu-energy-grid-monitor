import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from .exceptions import NoDataFoundError

class BaseQueryConfig(ABC):
    """An abstract class to manage the query configuration for each ingestor."""

    @abstractmethod
    def get_time_window(self) -> tuple[datetime, datetime]:
        """Returns the start and end datetime for the next API query, based on the current time."""
        pass

    def report_failure(self, error: Exception):
        """
        Updates the config based on the given exception.
        """
        pass

class DailyQueryConfig(BaseQueryConfig):
    """A config class to manage daily queries to the ENTSO-E API"""
    
    def get_time_window(self) -> tuple[datetime, datetime]:
        """
        Returns the 24-hour time window between midnight yesterday and
        midnight today/this morning.
        """
        now = datetime.now(timezone.utc)
        daily_increment = relativedelta(days=1)

        midnight_today = now.replace(hour=0, minute=0, second=0)
        midnight_tomorrow = midnight_today + daily_increment
        
        return (midnight_today, midnight_tomorrow)

class DailyAdaptableQueryConfig(BaseQueryConfig):
    def __init__(self, eic_code: str):
        self._eic_code = eic_code
        self._retry_queue: list[datetime] = []
        self._last_new_day_attempted: datetime.date | None = None

    def get_time_window(self) -> tuple[datetime | None, datetime | None]:
        """
        Returns the next time window to process.
        Prioritizes the retry queue. Only fetches a new day once.
        """
        
        # If possible, return the first entry in the retry queue
        if self._retry_queue:
            start_date_to_retry = self._retry_queue.pop(0)
            end_date_to_retry = start_date_to_retry + relativedelta(days=1)
            logging.info(f"Retrying missed day {start_date_to_retry.date()} for {self._eic_code}")
            return (start_date_to_retry, end_date_to_retry)

        # Otherwise, check if we've already tried midnight yesterday
        now = datetime.now(timezone.utc)
        start_of_today = now.replace(hour=0, minute=0, second=0)
        start_of_yesterday = start_of_today - relativedelta(days=1)
        
        if self._last_new_day_attempted == start_of_yesterday.date():
            return (None, None)
        
        # If not, then return midnight yesterday to midnight tonight
        self._last_new_day_attempted = start_of_yesterday.date()
        
        return (start_of_yesterday, start_of_today)

    def report_failure(self, start_time: datetime, error: Exception):
        """Adds a failed job to the retry queue."""
        if isinstance(error, NoDataFoundError):
            logging.warning(f"No data for {self._eic_code} on {start_time.date()}. Adding to retry queue.")
            self._retry_queue.append(start_time)