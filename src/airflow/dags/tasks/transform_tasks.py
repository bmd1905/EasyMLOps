from typing import Any, Dict

import pandas as pd
from utils.error_handling import TransformError
from utils.monitoring import PipelineMonitoring
from utils.transform_utils import enrich_record, generate_record_hash, validate_record

from airflow.decorators import task


@task()
def transform_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform and validate the data"""
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
                continue

            is_valid, error_message = validate_record(payload)

            if not is_valid:
                metrics["invalid_records"] += 1
                metrics["validation_errors"][error_message] = (
                    metrics["validation_errors"].get(error_message, 0) + 1
                )
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
        raise TransformError(f"Failed to transform data: {str(e)}")
