import io
import json
from datetime import datetime, timedelta

import pandas as pd
import pendulum

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.session import provide_session

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Add this function to create the MinIO connection if it doesn't exist
@provide_session
def create_minio_conn(session=None):
    """Create MinIO connection if it doesn't exist"""
    conn = session.query(Connection).filter(Connection.conn_id == "minio_conn").first()

    if not conn:
        conn = Connection(
            conn_id="minio_conn",
            conn_type="s3",
            host="minio",
            port=9000,
            login="minioadmin",
            password="minioadmin",
            extra={
                "aws_access_key_id": "minioadmin",
                "aws_secret_access_key": "minioadmin",
                "endpoint_url": "http://minio:9000",
                "region_name": "ap-southeast-1",
                "verify": False,
            },
        )
        session.add(conn)
        session.commit()


# Add this task to create bucket if it doesn't exist
@task()
def ensure_bucket_exists() -> None:
    """Ensure the required MinIO bucket exists"""
    try:
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        if not s3_hook.check_for_bucket("kafka"):
            s3_hook.create_bucket(bucket_name="kafka")

    except Exception as e:
        raise AirflowException(f"Failed to create bucket: {str(e)}")


@dag(
    dag_id="minio_data_pipeline",
    default_args=default_args,
    description="A simple pipeline to process data from MinIO",
    schedule="@daily",
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
    3. Saves the processed data back to MinIO
    """

    # Create connection if it doesn't exist
    create_minio_conn()

    # Ensure bucket exists before other tasks
    ensure_bucket_exists()

    @task()
    def load_from_minio() -> dict:
        """Load all JSON files from MinIO and return as a dictionary"""
        try:
            s3_hook = S3Hook(aws_conn_id="minio_conn")

            # Define multiple paths to check
            # current_time = datetime.now(tz=pendulum.timezone("UTC"))

            path_prefix = "topics/raw-events-topic/year=2024/month=11/day=27/hour=13/"

            all_data = []
            files_found = False

            # List all keys in the directory
            keys = s3_hook.list_keys(bucket_name="kafka", prefix=path_prefix)

            if keys:
                files_found = True
                print(f"Found {len(keys)} files in path: {path_prefix}")

                for key in keys:
                    # Read the file
                    data = s3_hook.read_key(key=key, bucket_name="kafka")
                    if not data:
                        continue

                    try:
                        # Load JSON data
                        json_data = json.loads(data)
                        if isinstance(json_data, list):
                            all_data.extend(json_data)
                        else:
                            all_data.append(json_data)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON from file {key}: {str(e)}")
                        continue

            if not files_found:
                raise AirflowException(
                    f"No JSON files found in any of the checked paths: {path_prefix}"
                )

            if not all_data:
                raise AirflowException("No valid JSON data found in any files")

            print(f"Successfully loaded {len(all_data)} records")
            return {"data": all_data}

        except Exception as e:
            raise AirflowException(f"Failed to load data from MinIO: {str(e)}")

    @task()
    def transform_data(raw_data: dict) -> dict:
        """Transform the data by adding processing metadata"""
        try:
            df = pd.DataFrame(raw_data["data"])

            # Add processing metadata
            df["processed_date"] = datetime.now(tz=pendulum.timezone("UTC")).isoformat()
            df["processing_pipeline"] = "minio_etl"

            # Return as a single dictionary with 'data' key
            return {"data": df.to_dict(orient="records")}

        except Exception as e:
            raise AirflowException(f"Failed to transform data: {str(e)}")

    @task()
    def save_to_minio(processed_data: dict) -> None:
        """Save processed data back to MinIO"""
        try:
            # Initialize S3 Hook
            s3_hook = S3Hook(aws_conn_id="minio_conn")

            # Convert to DataFrame and then to JSON
            df = pd.DataFrame(processed_data["data"])
            json_buffer = io.StringIO()
            df.to_json(json_buffer, orient="records", lines=True)

            # Generate output path with timestamp
            output_path = (
                f'processed/output_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            )

            # Upload to MinIO
            s3_hook.load_string(
                string_data=json_buffer.getvalue(),
                key=output_path,
                bucket_name="kafka",
                replace=True,
            )

        except Exception as e:
            raise AirflowException(f"Failed to save data to MinIO: {str(e)}")

    # Define the task dependencies
    raw_data = load_from_minio()
    processed_data = transform_data(raw_data)
    save_to_minio(processed_data)


# Create DAG instance
minio_etl_dag = minio_etl()
