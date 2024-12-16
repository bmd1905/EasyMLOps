import logging
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pendulum
from ray_provider.decorators import ray

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ray configuration
CONN_ID = "ray_conn"
FOLDER_PATH = Path(__file__).parent / "ray_scripts"
RAY_TASK_CONFIG = {
    "conn_id": CONN_ID,
    "runtime_env": {
        "working_dir": str(FOLDER_PATH),
        "pip": [
            "ray[train]==2.9.0",
            "xgboost_ray==0.1.19",
            "xgboost==2.0.3",
            "pandas==1.3.0",
            "astro-provider-ray==0.3.0",
            "boto3>=1.34.90",
            "pyOpenSSL==23.2.0",
            "cryptography==41.0.7",
            "urllib3<2.0.0",
            "tensorboardX==2.6.2",
            "pyarrow",
        ],
    },
    "num_cpus": 3,
    "num_gpus": 0,
    "poll_interval": 5,
    "xcom_task_key": "dashboard",
}

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
def load_training_data() -> Dict[str, Any]:
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

        # Return as dictionary with 'data' key
        return {"data": df.to_dict(orient="records")}
    except Exception as e:
        raise AirflowException(f"Failed to load training data: {e}")


@ray.task(config=RAY_TASK_CONFIG)
def train_model_with_ray(data):
    """Train XGBoost model using Ray"""
    import pandas as pd
    import pyarrow.fs

    import ray
    from airflow.exceptions import AirflowException
    from ray.train import CheckpointConfig, FailureConfig, RunConfig, ScalingConfig
    from ray.train.xgboost import XGBoostTrainer

    try:
        # Convert data back to Ray dataset - now access the 'data' key
        df = pd.DataFrame(data["data"])
        dataset = ray.data.from_pandas(df)

        # Remove nulls and split data
        dataset = dataset.filter(lambda x: x is not None)
        train_dataset, valid_dataset = dataset.train_test_split(test_size=0.3)

        # MinIO with allow_bucket_creation configuration
        fs = pyarrow.fs.S3FileSystem(
            endpoint_override="http://minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            allow_bucket_creation=True,
        )

        # Configure training
        run_config = RunConfig(
            storage_filesystem=fs,
            storage_path="model-checkpoints-bucket/xgb_model",
            name="xgb_model_training",
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
                num_workers=2,
                use_gpu=False,
                resources_per_worker={"CPU": 2},
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
                "xgboost_ray": {
                    "ray_params": {
                        "elastic_training": False,
                        "max_failed_actors": 0,
                        "max_actor_restarts": 0,
                        "placement_options": {"skip_placement": True},
                    }
                },
            },
            datasets={"train": train_dataset, "valid": valid_dataset},
        )

        result = trainer.fit()

        # Return metrics and checkpoint path explicitly
        return {"metrics": result.metrics, "checkpoint_path": result.checkpoint.path}

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
    # Load data
    data = load_training_data()

    # Train model using Ray
    train_model_with_ray(data)


# Create DAG instance
training_pipeline_dag = training_pipeline()
