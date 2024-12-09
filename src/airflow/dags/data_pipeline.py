import logging
from datetime import timedelta
from typing import Any, Dict

import pendulum
from config.data_pipeline_config import DataPipelineConfig
from tasks.bronze.ingest_raw_data import (
    check_minio_connection,
    ingest_raw_data,
)
from tasks.bronze.validate_raw_data import validate_raw_data
from tasks.silver.transform_data import transform_data
from tasks.gold.load_to_dwh import load_dimensions_and_facts

from airflow.decorators import dag, task_group

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@task_group(group_id="bronze_layer")
def bronze_layer(config: DataPipelineConfig) -> Dict[str, Any]:
    """Task group for the bronze layer of the data pipeline."""

    # Check MinIO connection
    valid = check_minio_connection()
    if not valid:
        logger.error("MinIO connection failed.")
        return None

    # Ingest raw data
    raw_data = ingest_raw_data(config, valid)
    if raw_data is None:
        logger.error("Ingested raw data is None.")
        return None

    # Validate raw data
    validated_data = validate_raw_data(raw_data)
    if validated_data is None:
        logger.error("Validation of raw data failed.")
        return None

    return validated_data


@task_group(group_id="silver_layer")
def silver_layer(validated_data: Dict[str, Any]) -> Dict[str, Any]:
    """Task group for the silver layer of the data pipeline."""
    if validated_data is None:
        logger.error("Validated data is None.")
        return None

    # Transform data
    transformed_data = transform_data(validated_data)
    if transformed_data is None:
        logger.error("Data transformation failed.")
        return None

    return transformed_data


@task_group(group_id="gold_layer")
def gold_layer(transformed_data: Dict[str, Any]) -> bool:
    """Task group for the gold layer of the data pipeline."""
    if transformed_data is None:
        logger.error("Transformed data is None.")
        return False

    # Load dimensions and facts
    success = load_dimensions_and_facts(transformed_data)
    if not success:
        logger.error("Failed to load dimensional model.")
        return False

    return True


@dag(
    dag_id="data_pipeline",
    default_args=default_args,
    description="Data pipeline",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data_lake", "data_warehouse"],
)
def data_pipeline():
    """Main DAG function that orchestrates the data pipeline."""
    # Load configuration
    config = DataPipelineConfig.from_airflow_variables()

    # Execute bronze layer and get validated data
    validated_data = bronze_layer(config)

    # Pass validated data to silver layer
    transformed_data = silver_layer(validated_data)

    # Pass transformed data to gold layer
    gold_layer(transformed_data)


# Create DAG instance
data_pipeline_dag = data_pipeline()
