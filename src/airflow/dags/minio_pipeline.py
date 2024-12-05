import io
import json
from datetime import datetime, timedelta

import pandas as pd
import pendulum

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
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


@provide_session
def create_postgres_dwh_conn(session=None):
    """Create PostgreSQL DWH connection if it doesn't exist"""
    conn = (
        session.query(Connection).filter(Connection.conn_id == "postgres_dwh").first()
    )

    if not conn:
        conn = Connection(
            conn_id="postgres_dwh",
            conn_type="postgres",
            host="postgres-dwh",
            schema="dwh",
            login="dwh",
            password="dwh",
            port=5432,
        )
        session.add(conn)
        session.commit()


@task()
def check_postgres_connection() -> None:
    """Check if PostgreSQL connection is working"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")
        postgres_hook.get_conn()  # This will try to establish a connection
        print("Successfully connected to PostgreSQL DWH")
    except Exception as e:
        raise AirflowException(f"Failed to connect to PostgreSQL DWH: {str(e)}")


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
    3. Saves the processed data to PostgreSQL DWH
    """

    # Create connections if they don't exist
    create_minio_conn()
    create_postgres_dwh_conn()

    # Check PostgreSQL connection
    check_postgres_connection()

    # Ensure bucket exists before other tasks
    ensure_bucket_exists()

    @task()
    def load_from_minio() -> dict:
        """Load all JSON files from MinIO and return as a dictionary"""
        try:
            s3_hook = S3Hook(aws_conn_id="minio_conn")

            # Define multiple paths to check
            # current_time = datetime.now(tz=pendulum.timezone("UTC"))

            # path_prefix = "topics/raw-events-topic/year=2024/month=11/day=27/hour=13/"
            path_prefix = (
                "topics/validated-events-topic/year=2024/month=12/day=04/hour=15"
            )

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
    def save_to_postgres(processed_data: dict) -> None:
        """Save the processed data to PostgreSQL DWH"""
        try:
            postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")
            
            # Create schema if it doesn't exist
            create_schema_sql = "CREATE SCHEMA IF NOT EXISTS dwh;"
            postgres_hook.run(create_schema_sql)
            
            # Convert the processed data back to a DataFrame
            df = pd.DataFrame(processed_data["data"])
            
            # Drop existing table and create new one with correct schema
            drop_table_sql = """
            DROP TABLE IF EXISTS dwh.processed_events;
            """
            postgres_hook.run(drop_table_sql)
            
            create_table_sql = """
            CREATE TABLE dwh.processed_events (
                device_id INTEGER,
                created TEXT,
                data TEXT,
                processed_date TIMESTAMP,
                processing_pipeline VARCHAR(255)
            );
            """
            postgres_hook.run(create_table_sql)
            
            # Save the DataFrame to PostgreSQL
            conn = postgres_hook.get_sqlalchemy_engine()
            df.to_sql(
                name='processed_events',
                con=conn,
                schema='dwh',
                if_exists='append',
                index=False
            )
            
            print(f"Successfully saved {len(df)} records to PostgreSQL")
            
        except Exception as e:
            raise AirflowException(f"Failed to save data to PostgreSQL: {str(e)}")

    # Define the task dependencies
    raw_data = load_from_minio()
    processed_data = transform_data(raw_data)
    save_to_postgres(processed_data)


# Create DAG instance
minio_etl_dag = minio_etl()
