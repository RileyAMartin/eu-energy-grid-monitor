from abc import ABC, abstractmethod
from datetime import datetime, timezone
from confluent_kafka import Producer
from ..api.client import BaseFetcher
from ..exceptions import InvalidIntervalError, NoDataFoundError
from ..query_configs import BaseQueryConfig
from eugrid_monitor_core.models import EntsoeEvent, DlqIngestionEvent
from eugrid_monitor_core.topics import DLQ_INGESTION
import logging

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
                self._producer.produce(
                    self.topic_name,
                    key=self._eic_code,
                    value=event.model_dump_json()
                )

        except InvalidIntervalError as e:
            logging.warning(f"Invalid query duration for {self._eic_code} ({start_time.isoformat()} - {end_time.isoformat()})")
            self._query_config.report_failure(start_time, e)
        except NoDataFoundError as e:
            logging.warning(f"No data found for {self._eic_code} at {start_time.isoformat()}")
            self._query_config.report_failure(start_time, e)

        # All other exceptions are added to the DLQ.
        except Exception as e:
            logging.error(f"Failed to process ingestion cycle for {self._eic_code}: {e}")
            try:
                dlq_event = DlqIngestionEvent(
                    eic_code=self._eic_code,
                    start_time=start_time,
                    end_time=end_time,
                    failed_at=datetime.now(timezone.utc),
                    error_type=type(e).__name__,
                    error_msg=str(e)
                )
                self._producer.produce(
                    topic=DLQ_INGESTION,
                    key=self._eic_code,
                    value=dlq_event.model_dump_json()
                )
            except Exception as dlq_e:
                logging.error(f"Couldn't produce error to DLQ: {dlq_e}", exc_info=True)
