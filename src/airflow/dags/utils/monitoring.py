import json
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class PipelineMonitoring:
    @staticmethod
    def log_metrics(metrics: Dict[str, Any]) -> None:
        """Log pipeline metrics"""
        logger.info("Pipeline Metrics:")
        logger.info(json.dumps(metrics, indent=2))

        # Add alerts for concerning metrics
        if metrics.get("invalid_records", 0) > metrics.get("valid_records", 0):
            logger.warning("High number of invalid records detected!")

        if metrics.get("duplicate_records", 0) > 100:
            logger.warning("High number of duplicate records detected!")
