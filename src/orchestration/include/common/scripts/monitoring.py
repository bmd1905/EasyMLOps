import json
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class PipelineMonitoring:
    @staticmethod
    def log_metrics(metrics: Dict[str, Any]) -> None:
        """Log pipeline metrics and send alerts"""
        logger.info("Pipeline Metrics:")
        logger.info(json.dumps(metrics, indent=2))

        # Add more sophisticated alerting
        alerts = []

        # Data quality alerts
        if metrics.get("invalid_records", 0) > metrics.get("valid_records", 0):
            alerts.append("High number of invalid records detected!")

        if metrics.get("duplicate_records", 0) > 100:
            alerts.append("High number of duplicate records detected!")

        # Volume alerts
        if metrics.get("total_records", 0) < 100:
            alerts.append("Low data volume detected!")

        if metrics.get("total_records", 0) > 1000000:
            alerts.append("Unusually high data volume detected!")

        # Processing time alerts
        if metrics.get("processing_time", 0) > 3600:
            alerts.append("Long processing time detected!")

        # Send alerts if any
        if alerts:
            alert_message = "\n".join(alerts)
            logger.warning(f"Pipeline Alerts:\n{alert_message}")
            # Add integration with alerting system (e.g., Slack, email)
