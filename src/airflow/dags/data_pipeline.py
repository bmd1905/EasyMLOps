import logging
from datetime import timedelta

import pendulum
from config.data_pipeline_config import DataPipelineConfig
from tasks.minio_tasks import check_minio_connection, load_from_minio
from tasks.postgres_tasks import check_postgres_connection, save_to_postgres
from tasks.transform_tasks import transform_data

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException

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


@task_group(group_id="connection_checks")
def check_connections():
    """TaskGroup for checking all required connections"""
    # Run connection checks
    postgres_check = check_postgres_connection()
    minio_check = check_minio_connection()

    @task()
    def check_prerequisites(postgres_ok: bool, minio_ok: bool) -> None:
        """Check if all prerequisites are met before running the pipeline"""
        if not (postgres_ok and minio_ok):
            raise AirflowException(
                "Prerequisites not met: Database or MinIO connection not available"
            )

    # Set up dependencies within the connection check group
    check_prerequisites(postgres_check, minio_check)


@task_group(group_id="data_processing")
def process_data(config: DataPipelineConfig):
    """TaskGroup for data processing steps"""
    # Define the main pipeline tasks
    raw_data = load_from_minio(config)
    processed_data = transform_data(raw_data)
    save_to_postgres(processed_data)


@dag(
    dag_id="data_pipeline",
    default_args=default_args,
    description="Data pipeline from Data Lake to Data Warehouse",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data_lake", "data_warehouse"],
)
def data_pipeline():
    """
    ### Data Pipeline

    This pipeline:
    1. Loads all JSON data from Data Lake
    2. Transforms the data by adding processing metadata
    3. Saves the processed data to Data Warehouse
    """

    # Load configuration
    config = DataPipelineConfig.from_airflow_variables()

    # Define task groups
    connection_checks = check_connections()
    data_processing = process_data(config)

    # Set up dependencies between task groups
    connection_checks >> data_processing


# Create DAG instance
data_pipeline_dag = data_pipeline()
