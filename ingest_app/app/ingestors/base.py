from abc import ABC, abstractmethod
from datetime import datetime, timezone
from confluent_kafka import Producer
from ..api.client import EntsoeClient
from ..exceptions import InvalidIntervalError, NoDataFoundError
from ..query_configs import RecentWindowQueryConfig
from eugrid_monitor_core.models import EntsoeEvent, DlqIngestionEvent
from eugrid_monitor_core.topics import DLQ_INGESTION
import logging

class BaseIngestor(ABC):
    """An abstract base class for all ENTSO-E ingestion services."""

    def __init__(self, producer: Producer, eic_code: str, client: EntsoeClient, query_config: RecentWindowQueryConfig):
        self._producer = producer
        self._eic_code = eic_code
        self._client = client
        self._query_config = query_config

    @abstractmethod
    def _parse_response(self, response_content: str) -> list[EntsoeEvent]:
        """Parses the XML response (market document) into a list of events."""
        pass

    @abstractmethod
    def _get_query_params(self, start_time: datetime, end_time: datetime) -> str:
        """Constructs the query parameters for this metric."""        
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
            start_time, end_time = self._query_config.get_time_window()
            if start_time is None:
                logging.debug(f"No new work for {self._eic_code} - skipping cycle.")
                return

            params = self._get_query_params(start_time, end_time)
            response_text = self._client.get_data(params)

            # Parse the data to get a list of events
            events = self._parse_response(response_text)
            if not events:
                logging.warning(f"No valid data found for EIC {self._eic_code}")
                return

            # Add the events to Kafka
            for event in events:
                self._produce_safe(
                        self.topic_name,
                        key=self._eic_code,
                        value=event.model_dump_json()
                    )

        except InvalidIntervalError as e:
            logging.warning(f"Invalid query duration for {self._eic_code} ({start_time.isoformat()} - {end_time.isoformat()})")
        except NoDataFoundError as e:
            logging.warning(f"No data found for {self._eic_code} at {start_time.isoformat()}")

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
                self._produce_safe(
                    topic=DLQ_INGESTION,
                    key=self._eic_code,
                    value=dlq_event.model_dump_json()
                )
            except Exception as dlq_e:
                logging.error(f"Couldn't produce error to DLQ: {dlq_e}", exc_info=True)

    def _produce_safe(self, topic, key, value):
        """Polls after producing an event to make it safer."""
        while True:
            try:
                self._producer.produce(topic, key=key, value=value)
                self._producer.poll(0)
                break
            except BufferError:
                self._producer.poll(1)