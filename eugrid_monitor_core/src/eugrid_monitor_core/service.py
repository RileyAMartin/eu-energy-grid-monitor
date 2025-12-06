import logging
import signal
import time
from abc import ABC, abstractmethod
from threading import Event

class ServiceWorker(ABC):
    """
    Interface for a worker that performs a cyclical task.
    """
    @abstractmethod
    def run_cycle(self) -> None:
        """Perform one unit of work."""
        pass

    def startup(self) -> None:
        """Set up any resources."""
        pass

    def shutdown(self) -> None:
        """Clean up any resources."""
        pass

class ServiceRunner():
    """
    Manages the lifecycle of a ServiceWorker.
    """
    def __init__(self, worker: ServiceWorker, sleep_interval: int = 0):
        self._worker = worker
        self._sleep_interval = sleep_interval
        self._stop_event = Event()
    
    def _handle_signal(self, signum, _frame):
        """
        Handler to catch Docker stopping signals.
        """
        logging.info(f"Received signal {signum}. Shutting down...")
        self._stop_event.set()
    
    def run(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # Signal handlers for Docker
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        logging.info("--- Service starting ---")

        try:
            self._worker.startup()

            # Main loop
            while not self._stop_event.is_set():
                cycle_start_time = time.time()
                
                try:
                    self._worker.run_cycle()                
                except Exception as e:
                    logging.error(f"Error in service loop: {e}", exc_info=True)

                if self._sleep_interval > 0 and not self._stop_event.is_set():
                    elapsed_time = time.time() - cycle_start_time
                    time_to_sleep = max(0, self._sleep_interval - elapsed_time)

                    logging.info(f"Sleeping for {time_to_sleep} seconds.")
                    self._stop_event.wait(time_to_sleep)

        finally:
            logging.info("--- Service stopping ---")
            self._worker.shutdown()
            logging.info("--- Service shutdown ---")
