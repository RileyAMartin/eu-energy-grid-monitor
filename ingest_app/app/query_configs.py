from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

class RecentWindowQueryConfig():
    def __init__(self, hours_to_fetch: int = 3):
        self._hours_to_fetch = hours_to_fetch
    
    def get_time_window(self) -> tuple[datetime, datetime]:
        """
        Returns the time window between the current hour and (current hour - hours to fetch).
        """
        end_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        start_time = end_time - relativedelta(hours=self._hours_to_fetch)
        
        return (start_time, end_time)