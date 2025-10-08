from abc import ABC, abstractmethod
from typing import List

class BaseProcessor(ABC):
    """An abstract class to process raw data from Kafka."""

    @abstractmethod
    def process(self, events: List[dict]):
        """Processes the raw data from the Kafka consumer and publishes it to the producer."""
        pass