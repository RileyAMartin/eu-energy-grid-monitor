from abc import ABC, abstractmethod
from datetime import datetime
from confluent_kafka import Producer
from api.client import BaseFetcher
from exceptions import InvalidIntervalError, NoDataFoundError
from .query_configs import BaseQueryConfig
import json
import logging
import requests

class BaseIngestor(ABC):
    """An abstract base class for all ENTSO-E ingestion services."""

    def __init__(self, producer: Producer, eic_code: str, fetcher: BaseFetcher, query_config: BaseQueryConfig, api_url: str):
        self._producer = producer
        self._eic_code = eic_code
        self._fetcher = fetcher
        self._query_config = query_config
        self._api_url = api_url

    @abstractmethod
    def _parse_response(self, response_content: str) -> list[dict]:
        """Parses the XML response (GenerationDocument) into a list of records."""
        pass

    @abstractmethod
    def _build_url(self, start_time: datetime, end_time: datetime) -> str:
        """Constructs the URL to use for the ENTSO-E API."""        
        pass

    @property
    @abstractmethod
    def topic_name(self) -> str:
        """The Kafka topic to public messages to."""
        pass

    def run_ingestion_cycle(self):
        """A single run of the ingestion logic for this EIC code."""
        try:
            # Get the XML data from the ENTSO-E API
            start_time, end_time = self._query_config.get_time_window()
            url_to_fetch = self._build_url(start_time, end_time)
            response = self._fetcher.fetch(url_to_fetch)

            # Parse the data to get a list of events
            events = self._parse_response(response)
            if not events:
                logging.warning(f"No valid data found for EIC {self._eic_code}")
                return
            
            # Add the events to Kafka
            for event in events:
                event['eic_code'] = self._eic_code
                event_json = json.dumps(event)
                self._producer.produce(
                    self.topic_name,
                    key=self._eic_code,
                    value=event_json
                )

        except InvalidIntervalError as e:
            logging.warning(f"Invalid query duration for {self._eic_code}: {e}")
            self._query_config.report_failure()
        except NoDataFoundError as e:
            logging.warning(f"ENTSO-E API found no data for {self._eic_code}.")
        except requests.HTTPError as e:
            if e.response.status_code in [401, 403]:
                logging.critical(f"Authentication failed for {self._eic_code}.")
            elif e.response.status_code >= 500:
                logging.error(f"Server error (5xx) for {self._eic_code}.")
            else:
                logging.error(f"Client error ({e.response.status_code}) for {self._eic_code}.")
        except requests.RequestException as e:
            logging.error(f"Network request failed for {self._eic_code}.")
        except Exception as e:
            logging.error(f"Unexpected error for EIC {self._eic_code}. Error: {e}", exc_info=True)
