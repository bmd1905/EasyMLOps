from datetime import datetime
from hashlib import sha256
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import pendulum
from common.scripts.monitoring import PipelineMonitoring
from loguru import logger

from airflow.decorators import task

logger = logger.bind(name=__name__)


def validate_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Validate a single record against business rules"""
    try:
        if not isinstance(record.get("price"), (int, float)):
            logger.error("Invalid price type for record: {}", record)
            return False, "Invalid price type"
        if record.get("price", 0) < 0:
            logger.error("Negative price for record: {}", record)
            return False, "Negative price"
        if not record.get("product_id"):
            logger.error("Missing product_id for record: {}", record)
            return False, "Missing product_id"
        return True, None
    except Exception as e:
        logger.exception("Error validating record: {}", record)
        return False, str(e)


def generate_record_hash(record: Dict[str, Any]) -> str:
    """Generate a unique hash for a record based on business keys"""
    key_fields = ["product_id", "timestamp", "user_id"]
    key_string = "|".join(str(record.get(field, "")) for field in key_fields)
    return sha256(key_string.encode()).hexdigest()


def enrich_record(record: Dict[str, Any], record_hash: str) -> Dict[str, Any]:
    """Add metadata to a record"""
    record["processed_date"] = datetime.now(tz=pendulum.timezone("UTC")).isoformat()
    record["processing_pipeline"] = "minio_etl"
    record["valid"] = "TRUE"
    record["record_hash"] = record_hash
    return record


@task()
def validate_raw_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate the raw data"""
    metrics = {
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "duplicate_records": 0,
        "validation_errors": {},
    }

    try:
        flattened_data = []
        seen_records = set()

        for record in raw_data["data"]:
            metrics["total_records"] += 1
            payload = record.get("payload", {})

            record_hash = generate_record_hash(payload)

            if record_hash in seen_records:
                metrics["duplicate_records"] += 1
                logger.warning("Duplicate record found: {}", record)
                continue

            is_valid, error_message = validate_record(payload)

            if not is_valid:
                metrics["invalid_records"] += 1
                metrics["validation_errors"][error_message] = (
                    metrics["validation_errors"].get(error_message, 0) + 1
                )
                logger.warning("Invalid record: {}. Error: {}", record, error_message)
                continue

            metrics["valid_records"] += 1
            seen_records.add(record_hash)

            enriched_payload = enrich_record(payload, record_hash)
            flattened_data.append(enriched_payload)

        # Log metrics
        PipelineMonitoring.log_metrics(metrics)

        df = pd.DataFrame(flattened_data)
        return {"data": df.to_dict(orient="records"), "metrics": metrics}

    except Exception as e:
        logger.exception("Failed to transform data")
        raise Exception(f"Failed to transform data: {str(e)}")
