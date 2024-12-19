import logging

import jinja2
import pandas as pd

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from include.common.scripts.sql_utils import load_sql_template
from include.config.tune_config import (
    CATEGORICAL_COLUMNS,
    DEFAULT_ARGS,
    FEATURE_COLUMNS,
    RAY_TASK_CONFIG,
)
from include.utils.ray_setup import patch_logging_proxy

# Apply patch before importing Ray-related modules
patch_logging_proxy()

from ray_provider.decorators import ray  # noqa: E402

logger = logging.getLogger(__name__)


@task()
def load_training_data() -> dict:
    """Load training data from DWH"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")

        # Load and render SQL template
        template = jinja2.Template(load_sql_template("queries/load_training_data.sql"))
        query = template.render(feature_columns=FEATURE_COLUMNS)

        df = postgres_hook.get_pandas_df(query)[:20]

        # Data preprocessing
        df["price"] = df["price"].astype(float)
        for col in CATEGORICAL_COLUMNS:
            df[col] = pd.Categorical(df[col]).codes

        return {"data": df.to_dict(orient="records")}
    except Exception as e:
        raise AirflowException(f"Failed to load training data: {e}")


@ray.task(config=RAY_TASK_CONFIG)
def tune_hyperparameters(data: dict) -> dict:
    """Tune XGBoost hyperparameters using Ray Tune"""
    import pandas as pd
    import pyarrow.fs

    from airflow.exceptions import AirflowException
    from include.config.tune_config import (
        FEATURE_COLUMNS,
        MINIO_CONFIG,
        TRAINING_CONFIG,
        TUNE_CONFIG,
        TUNE_SEARCH_SPACE,
    )
    from ray import train, tune
    from ray.tune.schedulers import ASHAScheduler
    from ray.tune.search.optuna import OptunaSearch

    try:
        # Convert data to DataFrame
        df = pd.DataFrame(data["data"])

        def train_xgboost(config):
            import xgboost as xgb

            # Split data
            train_data = xgb.DMatrix(df[FEATURE_COLUMNS], label=df["is_purchased"])

            # Train model with current config
            results = {}
            xgb.train(
                config,
                train_data,
                num_boost_round=TRAINING_CONFIG["num_boost_round"],
                evals=[(train_data, "train")],
                evals_result=results,
            )

            # Report metrics using train.report with rmse metric
            train.report(
                {
                    "train_rmse": results["train"]["rmse"][-1],
                    "train_logloss": results["train"]["rmse"][-1],
                }
            )

        # Configure tuning
        search_alg = OptunaSearch(
            metric="train_rmse",  # Updated metric name
            mode="min",
        )

        scheduler = ASHAScheduler(
            metric="train_rmse",  # Updated metric name
            mode="min",
            max_t=TUNE_CONFIG["max_epochs"],
            grace_period=TUNE_CONFIG["grace_period"],
        )

        # MinIO configuration
        fs = pyarrow.fs.S3FileSystem(**MINIO_CONFIG)

        # Run tuning with older API style
        tuner = tune.run(
            train_xgboost,
            name="xgb_tune",
            storage_filesystem=fs,
            storage_path=TRAINING_CONFIG["model_path"],
            config=TUNE_SEARCH_SPACE,
            num_samples=TUNE_CONFIG["num_trials"],
            scheduler=scheduler,
            search_alg=search_alg,
            verbose=2,  # Add verbosity for better logging
        )

        # Get best trial
        best_trial = tuner.get_best_trial("train_rmse", "min")  # Updated metric name

        return {
            "best_config": best_trial.config,
            "best_metrics": best_trial.last_result,
        }

    except Exception as e:
        raise AirflowException(f"Hyperparameter tuning failed: {e}")


@ray.task(config=RAY_TASK_CONFIG)
def train_final_model(data: dict, best_params: dict) -> dict:
    """Train final model with best parameters"""
    import pandas as pd
    import pyarrow.fs

    import ray
    from airflow.exceptions import AirflowException
    from include.config.tune_config import (
        MINIO_CONFIG,
        TRAINING_CONFIG,
        TUNE_SEARCH_SPACE,
    )
    from ray.train import CheckpointConfig, RunConfig, ScalingConfig
    from ray.train.xgboost import XGBoostTrainer

    try:
        # Validate and process parameters
        if not best_params or "best_config" not in best_params:
            # Convert search space to concrete values using median of ranges
            model_params = {
                k: v.sample() if hasattr(v, "sample") else v
                for k, v in TUNE_SEARCH_SPACE.items()
            }
        else:
            model_params = best_params["best_config"]

        # Ensure all parameters are concrete values
        for k, v in model_params.items():
            if hasattr(v, "sample"):
                model_params[k] = v.sample()

        # Setup similar to original training but with best params
        df = pd.DataFrame(data["data"])
        dataset = ray.data.from_pandas(df)

        # MinIO configuration
        fs = pyarrow.fs.S3FileSystem(**MINIO_CONFIG)

        run_config = RunConfig(
            storage_filesystem=fs,
            storage_path=TRAINING_CONFIG["model_path"],
            name="xgb_final_model",
            checkpoint_config=CheckpointConfig(
                checkpoint_frequency=1,
                num_to_keep=1,
            ),
            failure_config=ray.train.FailureConfig(max_failures=3),
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
            params=model_params,
            datasets={"train": dataset},
        )

        result = trainer.fit()
        return {
            "metrics": result.metrics,
            "checkpoint_path": result.checkpoint.path,
            "best_params": best_params or {"best_config": model_params},
        }

    except Exception as e:
        raise AirflowException(f"Final model training failed: {e}")


@task(trigger_rule="all_done")
def save_results(results: dict) -> dict:
    """Save training results and metrics"""

    from include.common.scripts.monitoring import PipelineMonitoring

    try:
        # Validate results
        if not results or not isinstance(results, dict):
            logger.error(f"Invalid results format received: {results}")
            # Return empty but valid metrics to prevent pipeline failure
            return {
                "train_metrics": {},
                "best_params": {},
                "model_path": "",
                "status": "failed",
                "error": f"Invalid results format: {results}",
            }

        metrics = {
            "train_metrics": results.get("metrics", {}),
            "best_params": results.get("best_params", {}),
            "model_path": results.get("checkpoint_path", ""),
            "status": "success",
        }

        try:
            PipelineMonitoring.log_metrics(metrics)
        except Exception as e:
            logger.error(f"Failed to log metrics: {e}")
            metrics["status"] = "partial_success"
            metrics["error"] = str(e)

        return metrics

    except Exception as e:
        logger.error(f"Failed to save results: {e}")
        logger.error(f"Input results: {results}")
        # Return error metrics instead of raising exception
        return {
            "train_metrics": {},
            "best_params": {},
            "model_path": "",
            "status": "error",
            "error": str(e),
        }


@dag(
    dag_id="training_pipeline",
    default_args=DEFAULT_ARGS,
    description="ML training pipeline with hyperparameter tuning",
    schedule="@weekly",
    catchup=False,
    tags=["training", "ml"],
)
def training_pipeline():
    """
    ### ML Training Pipeline with Hyperparameter Tuning
    This DAG handles the end-to-end training process:
    1. Load training data
    2. Tune hyperparameters using Ray Tune
    3. Train final model with best parameters
    4. Save results and metrics
    """
    # Load data
    data = load_training_data()

    # Tune hyperparameters
    best_params = tune_hyperparameters(data)

    # Train final model with dependencies
    results = train_final_model(data, best_params)

    # Save results (no need to set explicit dependencies)
    save_results(results)


# Create DAG instance
training_pipeline_dag = training_pipeline()
