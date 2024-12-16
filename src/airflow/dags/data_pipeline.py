from datetime import timedelta
from typing import Any, Dict

import pendulum
from config.data_pipeline_config import DataPipelineConfig
from loguru import logger
from tasks.bronze.ingest_raw_data import (
    check_minio_connection,
    ingest_raw_data,
)
from tasks.bronze.validate_raw_data import validate_raw_data
from tasks.gold.load_to_dwh import load_dimensions_and_facts
from tasks.silver.transform_data import transform_data

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup

logger = logger.bind(name=__name__)

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(hours=1),
    "sla": timedelta(hours=2),
}


def bronze_layer(config: DataPipelineConfig) -> Dict[str, Any]:
    """Task group for the bronze layer of the data pipeline."""

    # Add pre-execution checks
    @task(task_id="check_prerequisites")
    def check_prerequisites():
        """Check all prerequisites before starting the layer"""
        # Check MinIO connection
        valid = check_minio_connection()
        if not valid:
            raise AirflowException("MinIO connection failed")
        return valid

    # Add retries and timeouts to critical tasks
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=30),
    )
    def ingest_data(config: DataPipelineConfig, valid: bool) -> Dict[str, Any]:
        return ingest_raw_data(config, valid)

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )
    def validate_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        if raw_data is None:
            raise AirflowException("Raw data is None")
        return validate_raw_data(raw_data)

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
    description="Data pipeline for processing events from MinIO to DWH",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data_lake", "data_warehouse"],
    max_active_runs=1,
    doc_md=__doc__,
)
def data_pipeline():
    """
    ### Data Pipeline DAG

    This DAG processes data through three layers:
    * Bronze: Raw data ingestion and validation
    * Silver: Data transformation and enrichment
    * Gold: Loading to dimensional model

    Dependencies:
    * MinIO connection
    * Postgres DWH connection
    """
    # Load configuration
    config = DataPipelineConfig.from_airflow_variables()

    # Execute layers with proper error handling
    with TaskGroup("bronze_layer_group") as bronze_group:
        validated_data = bronze_layer(config)

    with TaskGroup("silver_layer_group") as silver_group:
        transformed_data = silver_layer(validated_data)

    with TaskGroup("gold_layer_group") as gold_group:
        success = gold_layer(transformed_data)  # noqa: F841

    # Add monitoring task
    @task(trigger_rule="all_done")
    def monitor_pipeline():
        """Monitor pipeline execution and send metrics"""
        # Add monitoring logic here
        pass

    # Define dependencies
    bronze_group >> silver_group >> gold_group >> monitor_pipeline()


# Create DAG instance
data_pipeline_dag = data_pipeline()
