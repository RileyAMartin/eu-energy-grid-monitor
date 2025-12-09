from eugrid_monitor_core.service import ServiceRunner
from .workers import StorageWorker

def main():
    worker = StorageWorker()
    runner = ServiceRunner(worker=worker, sleep_interval=0)
    runner.run()

if __name__ == "__main__":
    main()