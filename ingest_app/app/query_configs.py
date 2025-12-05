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

    def report_failure(self, start_time: datetime, error: Exception):
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
    def __init__(self):
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
            self._retry_queue.append(start_time)

class YearlyBackfillQueryConfig(BaseQueryConfig):
    """Query config to backfill data from up to a year prior."""

    def get_time_window(self) -> tuple[datetime, datetime]:
        """
        Returns the time window between the start of yesterday
        and exactly one year prior.
        """
        start_of_today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_of_yesterday = start_of_today - relativedelta(days=1)
        one_year_prior = start_of_yesterday - relativedelta(years=1)

        return (one_year_prior, start_of_yesterday)

    
class RollingBackfillQueryConfig(BaseQueryConfig):
    """
    Query config to fetch a rolling X-day window prior to today.
    Backfilling by X days means we avoid issues caused by downtime and issues
    with delays in data being uploaded to the API.
    """
    def __init__(self, days_to_backfill: int = 3):
        self._days_to_backfill = days_to_backfill

    def get_time_window(self) -> tuple[datetime, datetime]:
        """
        Returns the time window between midnight today and midnight X days ago.
        """
        end_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = end_time - relativedelta(days=self._days_to_backfill)

        return (start_time, end_time)

class RecentWindowQueryConfig(BaseQueryConfig):
    def __init__(self, hours_to_fetch: int = 3):
        self._hours_to_fetch = hours_to_fetch
    
    def get_time_window(self) -> tuple[datetime, datetime]:
        """
        Returns the time window between the current hour and (current hour - hours to fetch).
        """
        end_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        start_time = end_time - relativedelta(hours=self._hours_to_fetch)
        
        return (start_time, end_time)