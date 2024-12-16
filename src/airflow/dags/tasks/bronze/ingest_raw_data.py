import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from typing import Any, Dict, List, Set, Tuple

from botocore.config import Config
from config.data_pipeline_config import DataPipelineConfig
from loguru import logger

from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.log.logging_mixin import LoggingMixin

# Disable lineage tracking
S3Hook.disable_lineage_tracking()

logger = logger.bind(name=__name__)


@task()
def check_minio_connection() -> bool:
    """Check if MinIO connection is working"""
    try:
        boto3_config = Config(max_pool_connections=128)
        s3_hook = S3Hook(aws_conn_id="minio_conn", config=boto3_config)
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
        logger.warning(f"No checkpoint found or error loading checkpoint: {str(e)}")
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
        logger.error(f"Error saving checkpoint: {str(e)}")


def process_s3_object(
    s3_hook: S3Hook, bucket: str, key: str
) -> Tuple[str, List[Dict], int]:
    """Process a single S3 object with performance tracking"""
    try:
        # Get object data using S3Hook
        obj = s3_hook.get_key(key=key, bucket_name=bucket)
        if not obj:
            return key, [], 0

        # Read data and track bytes processed
        raw_data = obj.get()["Body"].read()
        bytes_processed = len(raw_data)

        data = raw_data.decode("utf-8")
        if not data:
            return key, [], bytes_processed

        json_data = json.loads(data)
        if isinstance(json_data, list):
            return key, json_data, bytes_processed
        return key, [json_data], bytes_processed

    except Exception as e:
        logger.error(f"Error processing file {key}: {str(e)}")
        return key, [], 0


def get_all_keys_paginated(s3_hook: S3Hook, bucket_name: str, prefix: str) -> Set[str]:
    """Use the S3 paginator to get all keys without loading them all at once."""
    client = s3_hook.get_conn()
    paginator = client.get_paginator("list_objects_v2")
    all_keys = set()

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        contents = page.get("Contents", [])
        for obj in contents:
            key = obj["Key"]
            all_keys.add(key)

    return all_keys


def chunk_list(data_list, chunk_size):
    """Yield successive chunks of given size from data_list."""
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i : i + chunk_size]


@task(
    retries=3,
    retry_delay=timedelta(minutes=1),
    max_active_tis_per_dag=16,
)
def ingest_raw_data(config: DataPipelineConfig, valid: bool = True) -> Dict[str, Any]:
    start_time = time.time()
    log = LoggingMixin().log
    try:
        s3_hook = S3Hook(aws_conn_id="minio_conn")

        # Get all keys using pagination
        log.info(f"Scanning for files in {config.path_prefix}...")
        all_keys = get_all_keys_paginated(
            s3_hook, config.bucket_name, config.path_prefix
        )

        checkpoint_key = get_checkpoint_key(config)
        all_keys.discard(checkpoint_key)  # Remove checkpoint file

        if not all_keys:
            raise Exception(f"No files found in path: {config.path_prefix}")

        log.info("Loading checkpoint data...")
        processed_keys = load_checkpoint(s3_hook, config)
        keys_to_process = list(all_keys - processed_keys)
        if not keys_to_process:
            log.info("No new files to process")
            return {"data": [], "skipped_files": len(processed_keys)}

        log.info(f"Found {len(keys_to_process)} new files to process")

        MAX_WORKERS = 8
        all_data = []
        processed_files = 0
        error_files = 0
        total_bytes_processed = 0
        newly_processed_keys = set()

        # Process keys in manageable chunks
        total_chunks = len(list(chunk_list(keys_to_process, chunk_size=1000)))
        current_chunk = 0

        for chunk in chunk_list(keys_to_process, chunk_size=1000):
            current_chunk += 1
            chunk_start_time = time.time()
            log.info(
                f"Processing chunk {current_chunk}/{total_chunks} ({len(chunk)} files)"
            )

            with ThreadPoolExecutor(
                max_workers=min(MAX_WORKERS, len(chunk))
            ) as executor:
                future_to_key = {
                    executor.submit(
                        process_s3_object,
                        s3_hook,
                        config.bucket_name,
                        key,
                    ): key
                    for key in chunk
                }

                chunk_processed = 0
                for future in as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        _, data, bytes_processed = future.result()
                        chunk_processed += 1
                        if chunk_processed % 100 == 0:  # Log every 100 files
                            log.info(
                                f"Processed {chunk_processed}/{len(chunk)} files in current chunk"
                            )

                        if data:
                            all_data.extend(data)
                            processed_files += 1
                            newly_processed_keys.add(key)
                            total_bytes_processed += bytes_processed
                        else:
                            error_files += 1
                    except Exception as e:
                        log.error(f"Error processing {key}: {str(e)}")
                        error_files += 1

            chunk_time = time.time() - chunk_start_time
            chunk_speed = (
                total_bytes_processed / chunk_time / 1024 / 1024
                if chunk_time > 0
                else 0
            )
            log.info(
                f"Chunk {current_chunk} completed in {chunk_time:.2f} seconds. Speed: {chunk_speed:.2f} MB/s"
            )

            # After each chunk, update checkpoint
            if newly_processed_keys:
                log.info("Updating checkpoint...")
                processed_keys.update(newly_processed_keys)
                save_checkpoint(s3_hook, config, processed_keys)

        skipped_files = (
            len(processed_keys) - processed_files
        )  # might not be exact for partial runs

        if not all_data and processed_files == 0:
            raise Exception("No valid JSON data found in any files")

        log.info(
            f"Successfully processed {processed_files} files, {error_files} files had errors, "
            f"skipped {skipped_files} previously processed files"
        )
        log.info(f"Total new records ingested: {len(all_data)}")
        total_time = time.time() - start_time
        log.info(f"Total ingestion time: {total_time:.2f} seconds")
        log.info(
            f"Average speed: {total_bytes_processed / total_time / 1024 / 1024:.2f} MB/s"
        )

        return {
            "data": all_data,
            "processed_files": processed_files,
            "error_files": error_files,
            "skipped_files": skipped_files,
        }

    except Exception as e:
        raise Exception(f"Failed to load data from MinIO: {str(e)}")
