from datetime import timedelta
from typing import Any, Dict

import pandas as pd
import pendulum
from data_pipeline.bronze.ingest_raw_data import (
    check_minio_connection,
    ingest_raw_data,
)
from data_pipeline.bronze.validate_raw_data import validate_raw_data
from data_pipeline.gold.load_to_dwh import load_dimensions_and_facts
from data_pipeline.silver.transform_data import transform_data
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from include.config.data_pipeline_config import DataPipelineConfig
from loguru import logger

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup

logger = logger.bind(name=__name__)

# Constants for Great Expectations
POSTGRES_CONN_ID = "postgres_dwh"
POSTGRES_SCHEMA = "dwh"
GX_DATA_CONTEXT = "include/gx"

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
        task_id="quality_check_raw_data",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )
    def validate_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        if raw_data is None:
            logger.warning("Raw data is None, skipping validation")
            return {"data": [], "metrics": {"total_records": 0, "valid_records": 0}}
        return validate_raw_data(raw_data)

    # Check MinIO connection
    valid = check_minio_connection()
    if not valid:
        logger.error("MinIO connection failed.")
        return {"data": [], "metrics": {"total_records": 0, "valid_records": 0}}

    # Ingest raw data
    raw_data = ingest_raw_data(config, valid)
    if raw_data is None:
        logger.error("Ingested raw data is None.")
        return {"data": [], "metrics": {"total_records": 0, "valid_records": 0}}

    # Validate raw data
    validated_data = validate_raw_data(raw_data)
    if validated_data is None:
        logger.error("Validation of raw data failed.")
        return {"data": [], "metrics": {"total_records": 0, "valid_records": 0}}

    return validated_data


def silver_layer(validated_data: Dict[str, Any]) -> Dict[str, Any]:
    """Task group for the silver layer of the data pipeline."""
    if validated_data is None or not validated_data["data"]:
        logger.warning("No valid data to transform")
        return {"data": [], "metrics": {"transformed_records": 0}}

    # Transform data
    transformed_data = transform_data(validated_data)
    if transformed_data is None:
        logger.error("Data transformation failed.")
        return {"data": [], "metrics": {"transformed_records": 0}}

    return transformed_data


def gold_layer(transformed_data: Dict[str, Any]) -> pd.DataFrame:
    """Task group for the gold layer of the data pipeline."""
    if transformed_data is None:
        logger.error("Transformed data is None.")
        return False

    # Load dimensions and facts
    transformed_data = load_dimensions_and_facts(transformed_data)
    if not transformed_data["success"]:
        logger.error("Failed to load dimensional model.")
        return False

    return transformed_data["data"]


@task
def debug_data(data: Dict[str, Any], layer: str):
    """Debug task to inspect data between layers"""
    if data and "data" in data:
        df = pd.DataFrame(data["data"])
        logger.info(f"=== {layer} Layer Data ===")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"Shape: {df.shape}")
        logger.info(f"First row: {df.iloc[0].to_dict()}")
    return data


@task
def quality_check_gold_data(df: pd.DataFrame):
    """Validate gold data using Great Expectations"""
    try:
        # Initialize Great Expectations validation
        gx_validate = GreatExpectationsOperator(
            task_id="quality_check_gold_data",
            data_context_root_dir="include/gx",
            dataframe_to_validate=df,
            data_asset_name="gold_data_asset",
            execution_engine="PandasExecutionEngine",
            expectation_suite_name="gold_layer_suite",
            return_json_dict=True,
            fail_task_on_validation_failure=False,
        )
        return gx_validate.execute(context={})

    except Exception as e:
        logger.error(f"Data quality check failed: {str(e)}")
        raise AirflowException(f"Data quality check failed: {str(e)}")


@dag(
    dag_id="data_pipeline",
    default_args=default_args,
    description="Data pipeline for processing events from MinIO to DWH",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data_lake", "data_warehouse"],
    max_active_runs=3,
    doc_md=__doc__,
)
def data_pipeline():
    """
    ### Data Pipeline DAG

    This DAG processes data through three layers:
    * Bronze: Raw data ingestion and validation
    * Silver: Data transformation and enrichment
    * Gold: Loading to dimensional model with data quality validation

    Dependencies:
    * MinIO connection
    * Postgres DWH connection
    * Great Expectations context
    """
    # Load configuration
    config = DataPipelineConfig.from_airflow_variables()

    # Execute layers with proper error handling
    with TaskGroup("bronze_layer_group") as bronze_group:
        validated_data = bronze_layer(config)
        # validated_data = debug_data(validated_data, "Bronze")

    with TaskGroup("silver_layer_group") as silver_group:
        transformed_data = silver_layer(validated_data)

    # Gold layer tasks
    with TaskGroup("gold_layer_group") as gold_group:
        gold_data = gold_layer(transformed_data)
        quality_check_gold_data(gold_data)

    # Define dependencies
    bronze_group >> silver_group >> gold_group


# Create DAG instance
data_pipeline_dag = data_pipeline()
