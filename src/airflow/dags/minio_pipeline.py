import json
import logging
import time
from datetime import timedelta
from typing import Any, Dict, List

import pandas as pd
import pendulum
import requests
from utils.db_utils import batch_insert_data, create_schema_and_table
from utils.transform_utils import enrich_record, generate_record_hash, validate_record

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Connection, Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.session import provide_session

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

# Replace hard-coded config with Variables
BUCKET_NAME = Variable.get("MINIO_BUCKET_NAME", default_var="validated-events-bucket")
PATH_PREFIX = Variable.get(
    "MINIO_PATH_PREFIX", default_var="topics/validated-events-topic/year=2024/month=12"
)
SCHEMA_REGISTRY_URL = Variable.get(
    "SCHEMA_REGISTRY_URL", default_var="http://schema-registry:8081"
)
SCHEMA_SUBJECT = Variable.get("SCHEMA_SUBJECT", default_var="raw-events-topic-schema")
BATCH_SIZE = int(Variable.get("BATCH_SIZE", default_var="1000"))


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
        if not s3_hook.check_for_bucket(BUCKET_NAME):
            s3_hook.create_bucket(bucket_name=BUCKET_NAME)

    except Exception as e:
        raise AirflowException(f"Failed to create bucket: {str(e)}")


def get_latest_schema_from_registry() -> Dict[str, Any]:
    """Fetch the latest schema from Schema Registry"""
    try:
        response = requests.get(
            f"{SCHEMA_REGISTRY_URL}/subjects/{SCHEMA_SUBJECT}/versions/latest",
            timeout=5,
        )
        response.raise_for_status()
        return json.loads(response.json()["schema"])
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Failed to fetch schema from registry: {str(e)}")
    except (KeyError, json.JSONDecodeError) as e:
        raise AirflowException(f"Invalid schema format received: {str(e)}")


def get_postgres_schema_from_avro() -> List[Dict[str, Any]]:
    """Convert Avro schema to PostgreSQL column definitions"""
    try:
        # Fetch schema from registry instead of file
        avro_schema = get_latest_schema_from_registry()

        # Map Avro types to PostgreSQL types
        type_mapping = {
            "string": "TEXT",
            "long": "BIGINT",
            "double": "DOUBLE PRECISION",
            "int": "INTEGER",
            "boolean": "BOOLEAN",
            "null": "NULL",
        }

        columns = []
        for field in avro_schema["fields"]:
            field_type = field["type"]
            # Handle union types (e.g., ["null", "string"])
            if isinstance(field_type, list):
                # Use the non-null type if available
                actual_type = next(
                    (t for t in field_type if t != "null"), field_type[0]
                )
                nullable = "null" in field_type
            else:
                actual_type = field_type
                nullable = False

            pg_type = type_mapping.get(actual_type, "TEXT")
            columns.append(
                {"name": field["name"], "type": pg_type, "nullable": nullable}
            )

        return columns
    except Exception as e:
        raise AirflowException(f"Failed to process Avro schema: {str(e)}")


@dag(
    dag_id="minio_data_pipeline",
    default_args=default_args,
    description="A simple pipeline to process data from MinIO",
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
            path_prefix = PATH_PREFIX

            all_data = []
            files_found = False

            # List all keys in the directory
            keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=path_prefix)

            if keys:
                files_found = True
                print(f"Found {len(keys)} files in path: {path_prefix}")

                for key in keys:
                    # Read the file
                    data = s3_hook.read_key(key=key, bucket_name=BUCKET_NAME)
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
        """Transform the data with validation, deduplication, and metrics"""
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
                    logger.info(f"Skipping duplicate record with hash: {record_hash}")
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

            logger.info(f"Processing metrics: {json.dumps(metrics, indent=2)}")
            df = pd.DataFrame(flattened_data)

            return {"data": df.to_dict(orient="records"), "metrics": metrics}

        except Exception as e:
            logger.error(f"Transform error: {str(e)}")
            raise AirflowException(f"Failed to transform data: {str(e)}")

    @task()
    def save_to_postgres(processed_data: dict) -> None:
        """Save the processed data to PostgreSQL DWH"""
        try:
            postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")
            df = pd.DataFrame(processed_data["data"])

            columns = [
                "event_time",
                "event_type",
                "product_id",
                "category_id",
                "category_code",
                "brand",
                "price",
                "user_id",
                "user_session",
                "processed_date",
                "processing_pipeline",
                "valid",
                "record_hash",
            ]

            df = df[columns]
            df = df.astype(
                {
                    "product_id": "int",
                    "category_id": "int",
                    "user_id": "int",
                    "price": "float",
                }
            )

            create_schema_and_table(postgres_hook)
            batch_insert_data(postgres_hook, df)

            logger.info(f"Total records processed: {processed_data['metrics']}")

        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            raise AirflowException(f"Failed to save data to PostgreSQL: {str(e)}")

    # Define the task dependencies
    raw_data = load_from_minio()
    processed_data = transform_data(raw_data)
    save_to_postgres(processed_data)


# Create DAG instance
minio_etl_dag = minio_etl()
