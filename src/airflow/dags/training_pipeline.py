import logging
from typing import Any, Dict

import jinja2
import pandas as pd
from include.common.scripts.sql_utils import load_sql_template
from include.config.tune_config import (
    CATEGORICAL_COLUMNS,
    DEFAULT_ARGS,
    FEATURE_COLUMNS,
    RAY_TASK_CONFIG,
)
from ray_provider.decorators import ray

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


@task()
def load_training_data() -> Dict[str, Any]:
    """Load training data from DWH"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")

        # Load and render SQL template
        template = jinja2.Template(load_sql_template("queries/load_training_data.sql"))
        query = template.render(feature_columns=FEATURE_COLUMNS)

        df = postgres_hook.get_pandas_df(query)

        # Data preprocessing
        df["price"] = df["price"].astype(float)
        for col in CATEGORICAL_COLUMNS:
            df[col] = pd.Categorical(df[col]).codes

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
    from config.tune_config import (
        MINIO_CONFIG,
        TRAINING_CONFIG,
        XGBOOST_PARAMS,
    )
    from ray.data import DataContext
    from ray.train import CheckpointConfig, FailureConfig, RunConfig, ScalingConfig
    from ray.train.xgboost import XGBoostTrainer

    try:
        # Enable verbose progress reporting
        DataContext.get_current().execution_options.verbose_progress = True

        # Convert data back to Ray dataset
        df = pd.DataFrame(data["data"])
        dataset = ray.data.from_pandas(df)

        # Remove nulls and split data
        dataset = dataset.filter(lambda x: x is not None)
        train_dataset, valid_dataset = dataset.train_test_split(
            test_size=TRAINING_CONFIG["test_size"]
        )

        # MinIO configuration
        fs = pyarrow.fs.S3FileSystem(**MINIO_CONFIG)

        # Configure training
        run_config = RunConfig(
            storage_filesystem=fs,
            storage_path=TRAINING_CONFIG["model_path"],
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
                num_workers=TRAINING_CONFIG["num_workers"],
                use_gpu=TRAINING_CONFIG["use_gpu"],
                resources_per_worker=TRAINING_CONFIG["resources_per_worker"],
            ),
            label_column="is_purchased",
            num_boost_round=TRAINING_CONFIG["num_boost_round"],
            params=XGBOOST_PARAMS,
            datasets={"train": train_dataset, "valid": valid_dataset},
        )

        result = trainer.fit()
        return {"metrics": result.metrics, "checkpoint_path": result.checkpoint.path}

    except Exception as e:
        raise AirflowException(f"Model training failed: {e}")


@dag(
    dag_id="training_pipeline",
    default_args=DEFAULT_ARGS,
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
