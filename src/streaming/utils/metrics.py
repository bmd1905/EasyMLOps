import time
from threading import Lock

from loguru import logger


class RequestCounter:
    def __init__(self, name: str = "default"):
        self.name = name
        self.success_count = 0
        self.failure_count = 0
        self.last_log_time = time.time()
        self.lock = Lock()
        self.processing_times = []
        self.samples_threshold = 1_000

    def increment_success(self):
        with self.lock:
            self.success_count += 1
            self._check_and_log()

    def increment_failure(self):
        with self.lock:
            self.failure_count += 1
            self._check_and_log()

    def add_processing_time(self, processing_time: float):
        with self.lock:
            self.processing_times.append(processing_time)
            if len(self.processing_times) >= self.samples_threshold:
                avg_time = sum(self.processing_times) / len(self.processing_times)
                logger.info(
                    f"[{self.name}] Average processing time over last {self.samples_threshold} records: {avg_time:.2f}ms"
                )
                self.processing_times = []  # Reset for next batch

    def _check_and_log(self):
        current_time = time.time()
        if current_time - self.last_log_time >= 1.0:
            total = self.success_count + self.failure_count
            logger.info(
                f"[{self.name}] Processed {total} records in the last second "
                f"(Success: {self.success_count}, Failure: {self.failure_count})"
            )
            self.success_count = 0
            self.failure_count = 0
            self.last_log_time = current_time
