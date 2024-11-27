import os
import uuid
from datetime import datetime

import pytz


class Processor:
    def __init__(self, event: dict):
        self.event = event

    @staticmethod
    def process_event(event: dict) -> dict:
        def _add_timestamp(record: dict) -> dict:
            record["collector_tstamp"] = datetime.now(tz=pytz.UTC).strftime(
                "%Y-%m-%d %H:%M:%S %z"
            )

            return record

        def _add_id(record: dict) -> dict:
            record["root_id"] = str(uuid.uuid4())

            return record

        def _add_collector_id(record: dict) -> dict:
            record["collector_id"] = os.environ["HOSTNAME"]

            return record

        processed_event = _add_timestamp(event)
        processed_event = _add_id(processed_event)
        processed_event = _add_collector_id(processed_event)

        return processed_event

    @property
    def processed_event(self) -> dict:
        return self.process_event(self.event)
