import json
import os
import time

import boto3
import requests
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# Constants
KAFKA_CONNECT_URL = "http://localhost:8083"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

CONNECTOR_CONFIGS = {
    "validated-events": {
        "name": "minio-validated-sink",
        "config_file": "src/streaming/connectors/config/minio-sink-connector.json",
        "topic_override": "validated-events-topic",
        "bucket_override": "validated-events-bucket",
    },
    "invalidated-events": {
        "name": "minio-invalidated-sink",
        "config_file": "src/streaming/connectors/config/minio-sink-connector.json",
        "topic_override": "invalidated-events-topic",
        "bucket_override": "invalidated-events-bucket",
    },
}


def get_minio_client():
    """Create and return a MinIO client"""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="ap-southeast-1",
    )


def ensure_bucket_exists(bucket_name: str) -> None:
    """Create MinIO bucket if it doesn't exist"""
    s3_client = get_minio_client()

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "404" or error_code == "NoSuchBucket":
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"Successfully created bucket: {bucket_name}")
            except Exception as create_error:
                raise Exception(
                    f"Failed to create bucket {bucket_name}: {str(create_error)}"
                )
        else:
            raise Exception(f"Error checking bucket {bucket_name}: {str(e)}")


def deploy_connector(connector_type: str) -> None:
    """Deploy a specific connector type"""
    if connector_type not in CONNECTOR_CONFIGS:
        raise ValueError(f"Unknown connector type: {connector_type}")

    connector_info = CONNECTOR_CONFIGS[connector_type]
    connector_name = connector_info["name"]
    bucket_name = connector_info["bucket_override"]

    # Ensure bucket exists before deploying connector
    try:
        ensure_bucket_exists(bucket_name)
    except Exception as e:
        print(f"Error ensuring bucket exists: {str(e)}")
        return

    # First, delete existing connector if it exists
    delete_connector(connector_name)
    time.sleep(2)  # Wait for deletion to complete

    # Read the connector configuration
    with open(connector_info["config_file"], "r") as f:
        connector_config = json.load(f)

    # Override topic and bucket name
    connector_config["name"] = connector_name
    connector_config["config"]["topics"] = connector_info["topic_override"]
    connector_config["config"]["s3.bucket.name"] = bucket_name

    # Substitute environment variables
    connector_config = substitute_env_vars(connector_config)

    print(
        f"Deploying {connector_name} with config:",
        json.dumps(connector_config, indent=2),
    )

    # Deploy the connector through Kafka Connect REST API
    response = requests.post(
        f"{KAFKA_CONNECT_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(connector_config),
    )

    if response.status_code == 201:
        print(f"{connector_name} deployed successfully")
        # Wait a bit for the connector to start
        time.sleep(5)
        # Check the status
        check_connector_status(connector_name)
    else:
        print(f"Failed to deploy connector: {response.text}")


def deploy_all_connectors() -> None:
    """Deploy all configured connectors"""
    for connector_type in CONNECTOR_CONFIGS:
        print(f"\nDeploying {connector_type} connector...")
        try:
            deploy_connector(connector_type)
        except Exception as e:
            print(f"Error deploying {connector_type} connector: {str(e)}")


def substitute_env_vars(config):
    """Replace environment variables in the config"""
    if isinstance(config, dict):
        return {k: substitute_env_vars(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [substitute_env_vars(v) for v in config]
    elif isinstance(config, str) and config.startswith("${") and config.endswith("}"):
        env_var = config[2:-1]
        return os.environ.get(env_var, config)
    return config


def check_connector_status(connector_name):
    response = requests.get(f"http://localhost:8083/connectors/{connector_name}/status")
    if response.status_code == 200:
        status = response.json()
        print(f"Connector Status: {json.dumps(status, indent=2)}")
        return status
    else:
        print(f"Failed to get connector status: {response.text}")
        return None


def delete_connector(connector_name):
    response = requests.delete(f"http://localhost:8083/connectors/{connector_name}")
    if response.status_code in [204, 404]:
        print(f"Connector {connector_name} deleted successfully or didn't exist")
        return True
    else:
        print(f"Failed to delete connector: {response.text}")
        return False


if __name__ == "__main__":
    deploy_all_connectors()
