import requests
from abc import ABC, abstractmethod
from ..exceptions import InvalidIntervalError, NoDataFoundError

class BaseFetcher(ABC):
    """An abstract base class for all API fetchers."""

    @abstractmethod
    def fetch(self, url_to_fetch: str) -> str:
        """Fetches the content from the URL and returns the response text."""
        pass

class EntsoeApiFetcher(BaseFetcher):
    """A class to handle calls made to the ENTSO-E API."""

    def __init__(self, api_key: str):
        self._api_key = api_key

    def fetch(self, url_to_fetch: str) -> str:
        """Fetches the content from the ENTSO-E API and returns the response text."""
        # Append the API key to the URL
        url_to_fetch = f"{url_to_fetch}&securityToken={self._api_key}"
        
        response = requests.get(url_to_fetch, timeout=30)

        if "Delivered time interval is not valid" in response.text:
            raise InvalidIntervalError("The API rejected the time interval as too short.")

        if "No matching data found for" in response.text:
            raise NoDataFoundError("The API found no data for this metric during the given time interval.")

        response.raise_for_status()
        
        return response.content
