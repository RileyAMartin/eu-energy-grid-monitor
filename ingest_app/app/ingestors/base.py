from abc import ABC, abstractmethod
from exceptions import InvalidIntervalError
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from confluent_kafka import KafkaProducer
from api.client import BaseFetcher
from configs import BaseQueryConfig
import json
import logging

class BaseIngestor(ABC):
    """An abstract base class for all ENTSO-E ingestion services."""

    def __init__(self, producer: KafkaProducer, eic_code: str, fetcher: BaseFetcher, config: BaseQueryConfig):
        self._producer = producer
        self._eic_code = eic_code
        self._fetcher = fetcher
        self._config = config

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

    def _run_ingestion_cycle(self):
        """A single run of the ingestion logic for this EIC code."""
        logging.info("--- Starting new ingestion cycle ---")

        try:
            # Get the XML data from the ENTSO-E API
            start_time, end_time = self._config.get_time_window()
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

        except Exception as e:
            logging.error(f"Unexpected error for EIC {self._eic_code}. Error: {e}")
            self._config.report_failure(e)
