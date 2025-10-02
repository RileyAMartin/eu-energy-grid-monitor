from abc import ABC, abstractmethod
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

class BaseQueryConfig(ABC):
    """An abstract class to manage the query configuration for each ingestor."""

    @abstractmethod
    def get_time_window(self) -> tuple[datetime, datetime]:
        """Returns the start and end datetime for the next API query, based on the current time."""
        pass

    def report_failure(self, error: Exception):
        """
        Updates the config based on the given exception.
        This isn't used right now but will be important when adding adaptive configs.
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
