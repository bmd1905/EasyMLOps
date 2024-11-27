from pyflink.metrics import Counter, Meter


class MetricsCollector:
    def __init__(self, runtime_context):
        self.event_counter = runtime_context.get_metrics_group().counter(
            "processed_events"
        )
        self.error_meter = runtime_context.get_metrics_group().meter(
            "processing_errors"
        )
