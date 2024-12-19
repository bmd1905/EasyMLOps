from ray_provider.decorators import ray

from include.config.tune_config import RAY_TASK_CONFIG


@ray.task(config=RAY_TASK_CONFIG)
def train_final_model(data: dict, best_params: dict) -> dict:
    """Train final model with best parameters"""

    from datetime import datetime

    import pandas as pd
    import pyarrow.fs

    import mlflow
    import ray
    from airflow.exceptions import AirflowException
    from include.config.tune_config import (
        MINIO_CONFIG,
        TRAINING_CONFIG,
        TUNE_CONFIG,
        TUNE_SEARCH_SPACE,
        XGBOOST_PARAMS,
    )
    from ray.air.integrations.mlflow import MLflowLoggerCallback
    from ray.train import CheckpointConfig, RunConfig, ScalingConfig
    from ray.train.xgboost import XGBoostTrainer

    try:
        # Set MLflow tracking URI
        mlflow.set_tracking_uri(TUNE_CONFIG["mlflow_tracking_uri"])

        # Create experiment if it doesn't exist or is deleted
        experiment_name = f"xgb_final_model_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment is None or experiment.lifecycle_stage == "deleted":
            mlflow.create_experiment(experiment_name)
            experiment = mlflow.get_experiment_by_name(experiment_name)

        # Merge base XGBoost params with best params
        if not best_params or "best_config" not in best_params:
            # Convert search space to concrete values using median of ranges
            tuning_params = {
                k: v.sample() if hasattr(v, "sample") else v
                for k, v in TUNE_SEARCH_SPACE.items()
            }
        else:
            tuning_params = best_params["best_config"]

        # Ensure all parameters are concrete values
        for k, v in tuning_params.items():
            if hasattr(v, "sample"):
                tuning_params[k] = v.sample()

        # Merge base params with tuning params
        model_params = {**XGBOOST_PARAMS, **tuning_params}

        # Setup similar to original training but with best params
        df = pd.DataFrame(data["data"])
        dataset = ray.data.from_pandas(df)

        # MinIO configuration
        fs = pyarrow.fs.S3FileSystem(**MINIO_CONFIG)

        run_config = RunConfig(
            storage_filesystem=fs,
            storage_path=TRAINING_CONFIG["model_path"],
            name=experiment_name,
            callbacks=[
                MLflowLoggerCallback(
                    tracking_uri=TUNE_CONFIG["mlflow_tracking_uri"],
                    experiment_name=experiment_name,
                    save_artifact=True,
                )
            ],
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

        # Transform metrics to match expected format
        metrics = {
            "train_rmse": result.metrics.get("train-rmse", float("inf")),
            "train_logloss": result.metrics.get("train-logloss", float("inf")),
            "train_error": result.metrics.get("train-error", float("inf")),
            "train_auc": result.metrics.get("train-auc", 0.0),
            "train_mae": result.metrics.get("train-mae", float("inf")),
        }

        return {
            "metrics": metrics,
            "checkpoint_path": result.checkpoint.path,
            "best_params": best_params or {"best_config": model_params},
        }

    except Exception as e:
        raise AirflowException(f"Final model training failed: {e}")
