import json
import logging
from datetime import timedelta
from typing import Any, Dict, Tuple

import pandas as pd
import pendulum
import ray
from common.scripts.monitoring import PipelineMonitoring
from ray.train import CheckpointConfig, FailureConfig, RunConfig, ScalingConfig
from ray.train.xgboost import XGBoostTrainer

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "start_date": pendulum.datetime(2024, 1, 1, tz="UTC"),
}


@task()
def init_ray() -> None:
    """Initialize Ray with proper configuration"""
    runtime_env = {
        "env_vars": {
            "RAY_memory_monitor_refresh_ms": "0",
            "RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE": "1",
        }
    }

    try:
        ray.init(
            runtime_env=runtime_env,
            _system_config={
                "object_spilling_config": json.dumps(
                    {"type": "filesystem", "params": {"directory_path": "/tmp/spill"}}
                )
            },
            object_store_memory=4 * 1024 * 1024 * 1024,  # 4GB
            _memory=8 * 1024 * 1024 * 1024,  # 8GB
        )
    except Exception as e:
        raise AirflowException(f"Ray initialization error: {e}")


@task()
def load_training_data() -> ray.data.Dataset:
    """Load training data from DWH"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")

        feature_columns = [
            "brand",
            "price",
            "event_weekday",
            "category_code_level1",
            "category_code_level2",
            "activity_count",
            "is_purchased",
        ]

        query = (
            f"SELECT {', '.join(feature_columns)} FROM dwh.vw_ml_purchase_prediction"
        )
        df = postgres_hook.get_pandas_df(query)

        # Data preprocessing
        df["price"] = df["price"].astype(float)

        categorical_columns = [
            "brand",
            "event_weekday",
            "category_code_level1",
            "category_code_level2",
        ]
        for col in categorical_columns:
            df[col] = pd.Categorical(df[col]).codes

        return ray.data.from_pandas(df)
    except Exception as e:
        raise AirflowException(f"Failed to load training data: {e}")


@task()
def prepare_datasets(
    dataset: ray.data.Dataset,
) -> Tuple[ray.data.Dataset, ray.data.Dataset]:
    """Prepare training and validation datasets"""
    try:
        # Remove nulls
        dataset = dataset.filter(lambda x: x is not None)

        # Split data
        train_dataset, valid_dataset = dataset.train_test_split(test_size=0.3)

        return train_dataset, valid_dataset
    except Exception as e:
        raise AirflowException(f"Failed to prepare datasets: {e}")


@task()
def train_model(datasets: Tuple[ray.data.Dataset, ray.data.Dataset]) -> Dict[str, Any]:
    """Train XGBoost model"""
    try:
        train_dataset, valid_dataset = datasets

        run_config = RunConfig(
            checkpoint_config=CheckpointConfig(
                checkpoint_frequency=1,
                num_to_keep=1,
            ),
            failure_config=FailureConfig(
                max_failures=0,
                fail_fast=True,
            ),
        )

        trainer = XGBoostTrainer(
            run_config=run_config,
            scaling_config=ScalingConfig(
                num_workers=1,
                use_gpu=False,
                resources_per_worker={
                    "CPU": 1,
                    "memory": 2 * 1024 * 1024 * 1024,
                },
            ),
            label_column="is_purchased",
            num_boost_round=20,
            params={
                "objective": "binary:logistic",
                "eval_metric": ["logloss", "error", "rmse", "mae", "auc"],
                "tree_method": "hist",
                "max_depth": 6,
                "eta": 0.3,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
            },
            datasets={"train": train_dataset, "valid": valid_dataset},
        )

        result = trainer.fit()
        metrics = result.metrics

        # Log metrics
        PipelineMonitoring.log_metrics(
            {"training_metrics": metrics, "model_version": result.checkpoint.path}
        )

        return metrics

    except Exception as e:
        raise AirflowException(f"Model training failed: {e}")


@task()
def cleanup_ray():
    """Cleanup Ray resources"""
    try:
        ray.shutdown()
    except Exception as e:
        logger.warning(f"Ray cleanup warning: {e}")


@dag(
    dag_id="training_pipeline",
    default_args=default_args,
    description="ML training pipeline for purchase prediction",
    schedule="@weekly",
    catchup=False,
    tags=["training", "ml"],
)
def training_pipeline():
    """
    ### ML Training Pipeline

    This DAG handles the end-to-end training process:
    1. Initialize Ray cluster
    2. Load and preprocess training data
    3. Train XGBoost model
    4. Monitor and log metrics
    """
    # Initialize Ray
    ray_init = init_ray()

    # Load and prepare data
    dataset = load_training_data()
    train_valid_datasets = prepare_datasets(dataset)

    # Train model
    metrics = train_model(train_valid_datasets)

    # Cleanup
    cleanup = cleanup_ray()

    # Define dependencies
    ray_init >> dataset >> train_valid_datasets >> metrics >> cleanup


# Create DAG instance
training_pipeline_dag = training_pipeline()
