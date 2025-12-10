import logging
from confluent_kafka import Producer
from eugrid_monitor_core.service import ServiceWorker

class IngestionOrchestrator(ServiceWorker):
    """
    A composite worker that manages sub-workers and handles
    shared resources (Kafka producers).
    """
    def __init__(self, workers: list[ServiceWorker], producer: Producer):
        self._workers = workers
        self._producer = producer

    def startup(self) -> None:
        logging.info("Starting up ingestion orchestrator...")
        for worker in self._workers:
            worker.startup()

    def run_cycle(self) -> None:
        """Run one cycle for each registered worker."""
        for worker in self._workers:
            try:
                worker.run_cycle()
            except Exception as e:
                logging.error(f"Error in worker {type(worker).__name__}: {e}", exc_info=True)

    def shutdown(self) -> None:
        logging.info("Shutting down ingestion orchestrator...")
        
        # Shut down all workers before shutting down internal resources
        for worker in self._workers:
            try:
                worker.shutdown()
            except Exception as e:
                logging.error(f"Error shutting down {type(worker).__name__}: {e}")

        logging.info("Flushing Shared Kafka Producer...")
        remaining = self._producer.flush()
        if remaining > 0:
            logging.warning(f"--- {remaining} messages failed to deliver to Kafka. ---")
        else:
            logging.info("Kafka producer flushed successfully.")