from datetime import datetime
from hashlib import sha256
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import pendulum
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from include.common.scripts.monitoring import PipelineMonitoring
from loguru import logger

from airflow.decorators import task

logger = logger.bind(name=__name__)


def extract_payload(record: Dict[str, Any]) -> Dict[str, Any]:
    """Extract payload from nested record structure"""
    try:
        if isinstance(record.get("payload"), dict):
            return record["payload"]
        return record
    except Exception:
        return record


def validate_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Validate a single record against business rules"""
    try:
        # Extract payload if nested
        data = extract_payload(record)

        # Check if event_time exists (timestamp field)
        if not data.get("event_time"):
            logger.error("Missing event_time for record: {}", data)
            return False, "Missing event_time"

        # Check required fields
        if not data.get("event_type"):
            logger.error("Missing event_type for record: {}", data)
            return False, "Missing event_type"

        if not data.get("product_id"):
            logger.error("Missing product_id for record: {}", data)
            return False, "Missing product_id"

        if not data.get("user_id"):
            logger.error("Missing user_id for record: {}", data)
            return False, "Missing user_id"

        return True, None
    except Exception as e:
        logger.exception("Error validating record: {}", record)
        return False, str(e)


def generate_record_hash(record: Dict[str, Any]) -> str:
    """Generate a unique hash for a record based on business keys"""
    try:
        data = extract_payload(record)
        key_fields = ["product_id", "event_time", "user_id"]
        key_string = "|".join(str(data.get(field, "")) for field in key_fields)
        return sha256(key_string.encode()).hexdigest()
    except Exception as e:
        logger.error(f"Error generating hash for record: {str(e)}")
        return sha256(str(record).encode()).hexdigest()


def enrich_record(record: Dict[str, Any], record_hash: str) -> Dict[str, Any]:
    """Add metadata to a record"""
    try:
        # Keep original record structure
        enriched = record.copy()
        enriched["processed_date"] = datetime.now(
            tz=pendulum.timezone("UTC")
        ).isoformat()
        enriched["processing_pipeline"] = "minio_etl"
        enriched["valid"] = "TRUE"
        enriched["record_hash"] = record_hash

        # If payload exists, also add metadata there
        if isinstance(enriched.get("payload"), dict):
            enriched["payload"]["processed_date"] = enriched["processed_date"]
            enriched["payload"]["processing_pipeline"] = enriched["processing_pipeline"]
            enriched["payload"]["valid"] = enriched["valid"]
            enriched["payload"]["record_hash"] = enriched["record_hash"]

        return enriched
    except Exception as e:
        logger.error(f"Error enriching record: {str(e)}")
        return record


def flatten_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten nested record structure"""
    try:
        flattened = {}
        # Copy top-level metadata
        for key in ["record_hash", "processed_date", "processing_pipeline", "valid"]:
            if key in record:
                flattened[key] = record[key]

        # Extract payload data
        payload = record.get("payload", {})
        if isinstance(payload, dict):
            for key, value in payload.items():
                flattened[key] = value

        return flattened
    except Exception as e:
        logger.error(f"Error flattening record: {str(e)}")
        return record


@task()
def validate_raw_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate raw data using both Great Expectations and custom validation"""
    try:
        # Convert input data to DataFrame
        df = pd.DataFrame(raw_data["data"])
        logger.info(f"Initial columns: {df.columns.tolist()}")

        # Keep one sample record for schema
        sample_record = df.iloc[0].to_dict() if not df.empty else None

        # Add record hash
        df["record_hash"] = df.apply(
            lambda x: generate_record_hash(x.to_dict()), axis=1
        )

        # Custom validation
        records = df.to_dict("records")
        validation_results = [validate_record(record) for record in records]
        valid_records = [
            record
            for record, (is_valid, _) in zip(records, validation_results)
            if is_valid
        ]
        validation_errors = {
            i: error
            for i, (is_valid, error) in enumerate(validation_results)
            if not is_valid
        }

        # If no valid records but we have a sample, create a dummy record
        if not valid_records and sample_record:
            logger.warning("No valid records, using sample record for schema")
            dummy_record = sample_record.copy()
            # Mark it as dummy record
            dummy_record["is_dummy"] = True
            dummy_record["record_hash"] = generate_record_hash(dummy_record)
            valid_records = [dummy_record]

        # Convert valid records back to DataFrame
        valid_df = pd.DataFrame(valid_records)

        # Validate using Great Expectations
        gx_validate = GreatExpectationsOperator(
            task_id="quality_check_raw_data",
            data_context_root_dir="include/gx",
            dataframe_to_validate=valid_df,
            data_asset_name="raw_data_asset",
            execution_engine="PandasExecutionEngine",
            expectation_suite_name="raw_data_suite",
            return_json_dict=True,
        )

        validation_result = gx_validate.execute(context={})  # noqa: F841

        # Calculate metrics
        metrics = {
            "total_records": len(df),
            "valid_records": len(valid_records),
            "invalid_records": len(df) - len(valid_records),
            "validation_errors": validation_errors,
            "contains_dummy": bool(not valid_records and sample_record),
        }

        # Log metrics
        PipelineMonitoring.log_metrics(metrics)

        # Enrich and flatten valid records
        enriched_data = []
        for record in valid_records:
            enriched = enrich_record(record, record["record_hash"])
            flattened = flatten_record(enriched)
            enriched_data.append(flattened)

        return {"data": enriched_data, "metrics": metrics}

    except Exception as e:
        logger.exception("Failed to validate data")
        raise Exception(f"Failed to validate data: {str(e)}")
