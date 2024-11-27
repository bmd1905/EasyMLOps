import json
import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()


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


def deploy_minio_connector():
    connector_name = "minio-sink"

    # First, delete existing connector if it exists
    delete_connector(connector_name)
    time.sleep(2)  # Wait for deletion to complete

    # Read the connector configuration
    with open("src/streaming/connectors/config/minio-sink-connector.json", "r") as f:
        connector_config = json.load(f)

    # Substitute environment variables
    connector_config = substitute_env_vars(connector_config)

    print("Deploying connector with config:", json.dumps(connector_config, indent=2))

    # Deploy the connector through Kafka Connect REST API
    response = requests.post(
        "http://localhost:8083/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(connector_config),
    )

    if response.status_code == 201:
        print("MinIO sink connector deployed successfully")
        # Wait a bit for the connector to start
        time.sleep(5)
        # Check the status
        check_connector_status(connector_name)
    else:
        print(f"Failed to deploy connector: {response.text}")


if __name__ == "__main__":
    deploy_minio_connector()
