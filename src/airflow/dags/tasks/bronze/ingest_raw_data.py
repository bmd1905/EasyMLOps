import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Set, Tuple
from datetime import timedelta

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


def get_checkpoint_key(config: DataPipelineConfig) -> str:
    """Generate checkpoint file key"""
    return f"{config.path_prefix}/_checkpoint.json"


def load_checkpoint(s3_hook: S3Hook, config: DataPipelineConfig) -> Set[str]:
    """Load processed keys from checkpoint file"""
    try:
        checkpoint_data = s3_hook.read_key(
            key=get_checkpoint_key(config), bucket_name=config.bucket_name
        )
        if checkpoint_data:
            return set(json.loads(checkpoint_data).get("processed_keys", []))
    except Exception as e:
        print(f"No checkpoint found or error loading checkpoint: {str(e)}")
    return set()


def save_checkpoint(
    s3_hook: S3Hook, config: DataPipelineConfig, processed_keys: Set[str]
) -> None:
    """Save processed keys to checkpoint file"""
    try:
        checkpoint_data = json.dumps({"processed_keys": list(processed_keys)})
        s3_hook.load_string(
            string_data=checkpoint_data,
            key=get_checkpoint_key(config),
            bucket_name=config.bucket_name,
            replace=True,
        )
    except Exception as e:
        print(f"Error saving checkpoint: {str(e)}")


def process_s3_object(s3_hook: S3Hook, bucket: str, key: str) -> Tuple[str, List[Dict]]:
    """Process a single S3 object"""
    try:
        data = s3_hook.read_key(key=key, bucket_name=bucket)
        if not data:
            return key, []

        json_data = json.loads(data)
        if isinstance(json_data, list):
            return key, json_data
        return key, [json_data]

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from file {key}: {str(e)}")
        return key, []
    except Exception as e:
        print(f"Error processing file {key}: {str(e)}")
        return key, []


@task(
    retries=3,
    retry_delay=timedelta(minutes=1),
    max_active_tis_per_dag=16,  # Limit concurrent executions
)
def ingest_raw_data(config: DataPipelineConfig, valid: bool = True) -> Dict[str, Any]:
    """Ingest raw data from MinIO with optimized performance and incremental processing"""
    try:
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        all_data = []
        processed_files = 0
        error_files = 0
        skipped_files = 0

        # Get list of all keys
        all_keys = set(
            s3_hook.list_keys(bucket_name=config.bucket_name, prefix=config.path_prefix)
        )

        # Remove checkpoint file from processing
        checkpoint_key = get_checkpoint_key(config)
        all_keys.discard(checkpoint_key)

        if not all_keys:
            raise Exception(f"No files found in path: {config.path_prefix}")

        # Load checkpoint to get previously processed keys
        processed_keys = load_checkpoint(s3_hook, config)

        # Get unprocessed keys
        keys_to_process = list(all_keys - processed_keys)

        if not keys_to_process:
            print("No new files to process")
            return {"data": [], "skipped_files": len(processed_keys)}

        print(f"Found {len(keys_to_process)} new files to process")

        # Process new files in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(32, len(keys_to_process))) as executor:
            future_to_key = {
                executor.submit(
                    process_s3_object, s3_hook, config.bucket_name, key
                ): key
                for key in keys_to_process
            }

            newly_processed_keys = set()
            # Collect results as they complete
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    _, data = future.result()
                    if data:
                        all_data.extend(data)
                        processed_files += 1
                        newly_processed_keys.add(key)
                    else:
                        error_files += 1
                except Exception as e:
                    print(f"Error processing {key}: {str(e)}")
                    error_files += 1

            # Update checkpoint with newly processed files
            if newly_processed_keys:
                processed_keys.update(newly_processed_keys)
                save_checkpoint(s3_hook, config, processed_keys)

        skipped_files = len(processed_keys - newly_processed_keys)

        if not all_data and not skipped_files:
            raise Exception("No valid JSON data found in any files")

        print(
            f"Successfully processed {processed_files} files, {error_files} files had errors, "
            f"skipped {skipped_files} previously processed files"
        )
        print(f"Total new records ingested: {len(all_data)}")

        return {
            "data": all_data,
            "processed_files": processed_files,
            "error_files": error_files,
            "skipped_files": skipped_files,
        }

    except Exception as e:
        raise Exception(f"Failed to load data from MinIO: {str(e)}")
