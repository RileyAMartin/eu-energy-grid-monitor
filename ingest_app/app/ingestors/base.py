from abc import ABC, abstractmethod
from exceptions import InvalidIntervalError
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import json
import logging

class BaseIngestor(ABC):
    """An abstract base class for all ENTSO-E ingestion services."""

    def __init__(self, producer, eic_code, fetcher):
        self._producer = producer
        self._eic_code = eic_code
        self._fetcher = fetcher
        self._config = {
            'query_duration_minutes': 15,
            'query_start_time': self._get_latest_15min_interval()[0]
        }

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

    def _increment_query_duration(self) -> None:
        """Updates the query duration upon an invalid XML response from the API."""
        if self._config["query_duration_minutes"] == 15:
            new_duration = 30
        elif self._config["query_duration_minutes"] == 30:
            new_duration = 60
        self._config["query_duration_minutes"] = new_duration
        self._config["query_start_time"] = self._get_latest_min_interval(self, new_duration)
        return

    def _get_latest_min_interval(self, mins) -> tuple[datetime, datetime]:
        """Returns the most recent time interval for the given amount of minutes.
            The interval will always be neatly divisible, depending on the amount of minutes.
            (E.g. 30 mins will only return times of xx:00 and xx:30).
        """
        now_utc = datetime.now(timezone.utc)
        minutes_to_subtract = now_utc.minute % mins

        end_of_interval = now_utc - relativedelta(
            minutes=minutes_to_subtract,
            seconds=now_utc.second,
            microseconds=now_utc.microsecond
        )
        start_of_interval = end_of_interval - relativedelta(minutes=mins)

        return start_of_interval, end_of_interval

    def _run_ingestion_cycle(self):
        """A single run of the ingestion logic for this EIC code."""
        logging.info("--- Starting new ingestion cycle ---")

        try:
            # Get the XML data from the ENTSO-E API
            url_to_fetch = self._build_url(
                self._config['query_start_time'],
                self._config['query_start_time'] + relativedelta(minutes='query_duration_minutes')
            )
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

            # Increment the start time and reset the query duration
            self._config['query_start_time'] += relativedelta(minutes=self._config['query_duration_minutes'])
            self._config['query_duration_minutes'] = 15

        except InvalidIntervalError as e:
            logging.error(f"Adapting duration for {self._eic_code} from {self._config["query_duration_minutes"]}mins.")
            self._increment_query_duration()
            return
        except Exception as e:
            logging.error(f"Unexpected error for EIC {self._eic_code}. Error: {e}")
