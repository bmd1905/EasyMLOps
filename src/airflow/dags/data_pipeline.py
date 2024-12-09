import logging
from datetime import timedelta

import pendulum
from config.data_pipeline_config import DataPipelineConfig
from tasks.bronze.ingest_raw_data import (
    check_minio_connection,
    ingest_raw_data,
)
from tasks.bronze.validate_raw_data import validate_raw_data

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
def bronze_layer(config: DataPipelineConfig):
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
    # Load configuration
    config = DataPipelineConfig.from_airflow_variables()

    # Bronze layer
    data_pipeline_result = bronze_layer(config)


# Create DAG instance
data_pipeline_dag = data_pipeline()
