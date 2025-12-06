import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, Any
from ..exceptions import InvalidIntervalError, NoDataFoundError

class EntsoeClient():
    """
    HTTP client for the ENTSO-E API.
    """
    def __init__(self, api_key: str, base_url: str = "https://web-api.tp.entsoe.eu/api"):
        self._api_key = api_key
        self._base_url = base_url
        self._session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Creates a Session to handle requests to the ENTSO-E API.
        """
        session = requests.Session()

        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )

        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def get_data(self, params: Dict[str, Any]) -> bytes:
        """
        Fetches data from the API based on the provided parameters.
        Returns the response content in byte format.
        """
        request_params = {
            "securityToken": self._api_key,
            **params
        }

        try:
            response = self._session.get(
                self._base_url,
                params=request_params,
                timeout=30
            )

            if "Delivered time interval is not valid" in response.text:
                raise InvalidIntervalError("The API rejected the time interval as too short.")
            if "No matching data found" in response.text:
                raise NoDataFoundError("The API found no data for this query.")

            response.raise_for_status()

            return response.content
        except requests.RequestException as e:
            logging.error(f"HTTP request failed: {e}")
            raise
