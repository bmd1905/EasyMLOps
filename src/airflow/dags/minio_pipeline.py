import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

from config.minio_config import MinioConfig
from tasks.minio_tasks import check_minio_connection, load_from_minio
from tasks.postgres_tasks import check_postgres_connection, save_to_postgres
from tasks.transform_tasks import transform_data

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


@task()
def check_prerequisites(postgres_ok: bool, minio_ok: bool) -> None:
    """Check if all prerequisites are met before running the pipeline"""
    if not (postgres_ok and minio_ok):
        raise AirflowException(
            "Prerequisites not met: Database or MinIO connection not available"
        )


@dag(
    dag_id="minio_data_pipeline",
    default_args=default_args,
    description="ETL pipeline from MinIO to PostgreSQL",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["minio", "etl"],
)
def minio_etl():
    """
    ### MinIO ETL Pipeline

    This pipeline:
    1. Loads all JSON data from MinIO
    2. Transforms the data by adding processing metadata
    3. Saves the processed data to PostgreSQL DWH
    """

    # Load configuration
    config = MinioConfig.from_airflow_variables()

    # Run connection checks
    postgres_check = check_postgres_connection()
    minio_check = check_minio_connection()

    # Check prerequisites
    prerequisites_met = check_prerequisites(postgres_check, minio_check)

    # Define the main pipeline tasks
    raw_data = load_from_minio(config)
    processed_data = transform_data(raw_data)
    save_task = save_to_postgres(processed_data)

    # Set up task dependencies
    prerequisites_met >> raw_data >> processed_data >> save_task


# Create DAG instance
minio_etl_dag = minio_etl()
