import logging
from datetime import timedelta
from typing import Any, Dict, Tuple

import pandas as pd
import pendulum
from common.scripts.monitoring import PipelineMonitoring

import ray
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from ray.train import CheckpointConfig, FailureConfig, RunConfig, ScalingConfig
from ray.train.xgboost import XGBoostTrainer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ray cluster configuration
RAY_ADDRESS = "ray://ray-head:10001"

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
def connect_ray() -> None:
    """Connect to Ray cluster"""
    try:
        ray.init(address=RAY_ADDRESS)
        logger.info("Successfully connected to Ray cluster")
    except Exception as e:
        raise AirflowException(f"Failed to connect to Ray cluster: {e}")


@task()
def disconnect_ray():
    """Disconnect from Ray cluster"""
    try:
        ray.shutdown()
        logger.info("Successfully disconnected from Ray cluster")
    except Exception as e:
        logger.warning(f"Ray disconnect warning: {e}")


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
    This DAG handles the end-to-end training process using a dedicated Ray cluster
    """
    # Connect to Ray cluster
    ray_connect = connect_ray()

    # Load and prepare data
    dataset = load_training_data()
    train_valid_datasets = prepare_datasets(dataset)

    # Train model
    metrics = train_model(train_valid_datasets)

    # Disconnect
    ray_disconnect = disconnect_ray()

    # Define dependencies
    ray_connect >> dataset >> train_valid_datasets >> metrics >> ray_disconnect


# Create DAG instance
training_pipeline_dag = training_pipeline()
