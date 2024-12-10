import json

from data_loader import load_data
from preprocessor import remove_nulls, split_data
from trainer import train_model

import ray


def init_ray():
    """Initialize Ray with proper configuration"""
    runtime_env = {
        "env_vars": {
            "RAY_memory_monitor_refresh_ms": "0",
            "RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE": "1",
            "RAY_ADDRESS": "ray-head:6379",
            "RAY_DISABLE_DOCKER_CPU_WARNING": "1",
        }
    }

    try:
        ray.init(
            runtime_env=runtime_env,
            _system_config={
                "object_spilling_config": json.dumps(
                    {"type": "filesystem", "params": {"directory_path": "/tmp/spill"}}
                ),
                "distributed_connect_timeout_s": 30,
                "distributed_connect_num_retries": 3,
            },
        )
    except Exception as e:
        print(f"Ray initialization error: {e}")
        raise


def main():
    # Initialize Ray
    init_ray()

    try:
        # Load and process data
        dataset = load_data()
        dataset = remove_nulls(dataset)
        train_dataset, valid_dataset = split_data(dataset)

        # Train model
        train_model(train_dataset, valid_dataset)
    except Exception as e:
        print(f"Error during execution: {e}")
        raise
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
