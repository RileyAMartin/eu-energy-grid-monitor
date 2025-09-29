from abc import ABC, abstractmethod
from datetime import datetime

class BaseAPIFetcher(ABC):
    """An abstract base class for all ENTSO-E API fetchers."""

    @abstractmethod
    def fetch(url_to_fetch: str) -> str:
        """Fetches the content from the ENTSO-E API and returns the response content."""
        pass