import logging

from airflow.decorators import task
from include.common.scripts.monitoring import PipelineMonitoring

logger = logging.getLogger(__name__)


@task(trigger_rule="all_done")
def save_results(results: dict) -> dict:
    """Save training results and metrics"""
    try:
        # Validate results
        if not results or not isinstance(results, dict):
            logger.error(f"Invalid results format received: {results}")
            # Return empty but valid metrics to prevent pipeline failure
            return {
                "train_metrics": {},
                "best_params": {},
                "model_path": "",
                "status": "failed",
                "error": f"Invalid results format: {results}",
            }

        metrics = {
            "train_metrics": results.get("metrics", {}),
            "best_params": results.get("best_params", {}),
            "model_path": results.get("checkpoint_path", ""),
            "status": "success",
        }

        try:
            PipelineMonitoring.log_metrics(metrics)
        except Exception as e:
            logger.error(f"Failed to log metrics: {e}")
            metrics["status"] = "partial_success"
            metrics["error"] = str(e)

        return metrics

    except Exception as e:
        logger.error(f"Failed to save results: {e}")
        logger.error(f"Input results: {results}")
        # Return error metrics instead of raising exception
        return {
            "train_metrics": {},
            "best_params": {},
            "model_path": "",
            "status": "error",
            "error": str(e),
        }
