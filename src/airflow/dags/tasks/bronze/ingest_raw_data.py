import json
from typing import Any, Dict

from config.data_pipeline_config import DataPipelineConfig

from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@task()
def check_minio_connection() -> bool:
    """Check if MinIO connection is working"""
    try:
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        s3_hook.get_conn()
        return True
    except Exception as e:
        raise Exception(f"Failed to connect to MinIO: {str(e)}")


@task()
def ingest_raw_data(config: DataPipelineConfig, valid: bool = True) -> Dict[str, Any]:
    """Ingest raw data from MinIO"""
    try:
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        all_data = []
        files_found = False

        keys = s3_hook.list_keys(
            bucket_name=config.bucket_name, prefix=config.path_prefix
        )

        if keys:
            files_found = True
            for key in keys:
                data = s3_hook.read_key(key=key, bucket_name=config.bucket_name)
                if data:
                    try:
                        json_data = json.loads(data)
                        if isinstance(json_data, list):
                            all_data.extend(json_data)
                        else:
                            all_data.append(json_data)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON from file {key}: {str(e)}")

        if not files_found:
            raise Exception(f"No files found in path: {config.path_prefix}")

        if not all_data:
            raise Exception("No valid JSON data found in any files")

        return {"data": all_data}

    except Exception as e:
        raise Exception(f"Failed to load data from MinIO: {str(e)}")
