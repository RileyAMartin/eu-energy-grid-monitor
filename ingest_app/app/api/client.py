import requests
from abc import ABC, abstractmethod
from exceptions import InvalidIntervalError

class BaseFetcher(ABC):
    """An abstract base class for all API fetchers."""

    @abstractmethod
    def fetch(self, url_to_fetch: str) -> str:
        """Fetches the content from the API and returns the response text."""
        pass

class EntsoeApiFetcher(BaseFetcher):
    """A class to handle calls made to the ENTSO-E API."""

    def __init__(self, api_key: str):
        self._api_key = api_key

    def fetch(self, url_to_fetch: str) -> str:
        """Fetchs the content from the ENTSO-E API and returns the response text."""
        response = requests.get(url_to_fetch, timeout=30)

        if "Delivered time interval is not valid" in response.text:
            raise InvalidIntervalError("The API rejected the time interval as too short.")

        response.raise_for_status()
        
        return response.text
