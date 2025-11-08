from abc import ABC, abstractmethod
from datetime import datetime
from confluent_kafka import Producer
from lxml import etree
from ..api.client import BaseFetcher
from ..exceptions import InvalidIntervalError, NoDataFoundError
from ..query_configs import BaseQueryConfig
from eugrid_monitor_core.models import EntsoeEvent, DlqErrorTypesEnum, DlqIngestionEvent
from eugrid_monitor_core.topics import DLQ_INGESTION
from pydantic import ValidationError
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
    def _parse_response(self, response_content: str) -> list[EntsoeEvent]:
        """Parses the XML response (market document) into a list of events."""
        pass

    @abstractmethod
    def _build_url(self, start_time: datetime, end_time: datetime) -> str:
        """Constructs the URL to use for the ENTSO-E API."""        
        pass

    @property 
    @abstractmethod
    def topic_name(self) -> str:
        """The Kafka topic name that this ingestor uses."""
        pass

    @property
    def dlq_topic_name(self) -> str:
        """The Kafka topic name of the DLQ for all ingestors."""
        return DLQ_INGESTION

    def _produce_dlq_event(self, dlq_event: DlqIngestionEvent):
        """Produces a DlqIngestionEvent to the DLQ."""
        try:
            self._producer.produce(
                topic=self.dlq_topic_name,
                key=self._eic_code,
                value=dlq_event.model_dump_json()
            )
        except Exception as e:
            logging.error(f"Error producing message to DLQ: {e}")

    def run_ingestion_cycle(self):
        """A single run of the ingestion logic for this EIC code."""
        try:
            # Get the XML data from the ENTSO-E API
            start_time, end_time = None, None
            start_time, end_time = self._query_config.get_time_window()

            # This interval has already been fetched
            if start_time is None:
                logging.debug(f"No new work for {self._eic_code} - skipping cycle.")
                return

            url_to_fetch = self._build_url(start_time, end_time)
            response = self._fetcher.fetch(url_to_fetch)

            # Parse the data to get a list of events
            events = self._parse_response(response)
            if not events:
                logging.warning(f"No valid data found for EIC {self._eic_code}")
                return

            # Add the events to Kafka
            for event in events:
                event_json = event.model_dump_json()
                self._producer.produce(
                    self.topic_name,
                    key=self._eic_code,
                    value=event_json
                )

        except InvalidIntervalError as e:
            logging.warning(f"Invalid query duration for {self._eic_code}: {e}")
            self._query_config.report_failure(start_time, e)
        except NoDataFoundError as e:
            self._query_config.report_failure(start_time, e)

        except Exception as e:
            # All other exceptions are added to the DLQ.
            logging.error(f"Failed to process ingestion cycle for {self._eic_code}: {e}")

            error_type = DlqErrorTypesEnum.OTHER
            error_msg = str(e)

            if isinstance(e, requests.HTTPError):
                error_type = DlqErrorTypesEnum.NETWORK
                error_msg = e.response.text
            elif isinstance(e, requests.RequestException):
                error_type = DlqErrorTypesEnum.NETWORK
            elif isinstance(e, ValidationError):
                error_type = DlqErrorTypesEnum.VALIDATION
            elif isinstance(e, etree.LxmlError):
                error_type = DlqErrorTypesEnum.PARSING

            dlq_event = DlqIngestionEvent(
                eic_code=self._eic_code,
                start_time=start_time,
                end_time=end_time,
                error_type=error_type,
                error_msg=error_msg
            )
            self._produce_dlq_event(dlq_event)
