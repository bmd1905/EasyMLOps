from include.config.tune_config import RAY_TASK_CONFIG
from ray_provider.decorators import ray


@ray.task(config=RAY_TASK_CONFIG)
def tune_hyperparameters(data: dict) -> dict:
    """Tune XGBoost hyperparameters using Ray Tune"""

    from datetime import datetime

    import mlflow
    import pandas as pd
    import pyarrow.fs
    from airflow.exceptions import AirflowException
    from include.config.tune_config import (
        MINIO_CONFIG,
        TRAINING_CONFIG,
        TUNE_CONFIG,
        TUNE_SEARCH_SPACE,
        XGBOOST_PARAMS,
    )

    import ray
    from ray import train, tune
    from ray.air.integrations.mlflow import MLflowLoggerCallback
    from ray.train import CheckpointConfig, RunConfig, ScalingConfig
    from ray.train.xgboost import XGBoostTrainer
    from ray.tune.schedulers import ASHAScheduler
    from ray.tune.search.optuna import OptunaSearch

    try:
        # Set MLflow tracking URI
        mlflow.set_tracking_uri(TUNE_CONFIG["mlflow_tracking_uri"])

        # Create experiment if it doesn't exist or is deleted
        experiment_name = f"xgb_tune_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment is None or experiment.lifecycle_stage == "deleted":
            mlflow.create_experiment(experiment_name)
            experiment = mlflow.get_experiment_by_name(experiment_name)

        # Convert data to DataFrame
        df = pd.DataFrame(data["data"])

        # Convert data to Ray dataset
        dataset = ray.data.from_pandas(df)

        def train_xgboost(config):
            # Merge base XGBoost params with tuning config
            training_params = {**XGBOOST_PARAMS, **config}

            # Create trainer
            trainer = XGBoostTrainer(
                run_config=run_config,
                scaling_config=ScalingConfig(
                    num_workers=TRAINING_CONFIG["num_workers"],
                    use_gpu=TRAINING_CONFIG["use_gpu"],
                    resources_per_worker=TRAINING_CONFIG["resources_per_worker"],
                ),
                label_column="is_purchased",
                num_boost_round=TRAINING_CONFIG["num_boost_round"],
                params=training_params,  # Use merged params
                datasets={"train": dataset},
            )

            # Train model with current config
            results = trainer.fit()

            # Report metrics using train.report with rmse metric
            train.report(
                {
                    "train_logloss": results.metrics.get("train-logloss", float("inf")),
                    "train_error": results.metrics.get("train-error", float("inf")),
                    "train_rmse": results.metrics.get("train-rmse", float("inf")),
                    "train_mae": results.metrics.get("train-mae", float("inf")),
                    "train_auc": results.metrics.get("train-auc", 0.0),
                }
            )

        # Configure tuning
        search_alg = OptunaSearch(
            metric="train_rmse",
            mode="min",
        )

        scheduler = ASHAScheduler(
            metric="train_rmse",
            mode="min",
            max_t=TUNE_CONFIG["max_epochs"],
            grace_period=TUNE_CONFIG["grace_period"],
        )

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
                    save_artifact=False,
                )
            ],
            checkpoint_config=CheckpointConfig(
                checkpoint_frequency=1,
                num_to_keep=1,
            ),
            failure_config=ray.train.FailureConfig(max_failures=3),
        )

        # Run tuning with updated API
        tuner = tune.run(
            train_xgboost,
            storage_filesystem=fs,
            storage_path=TUNE_CONFIG["model_path"],
            name=experiment_name,
            config=TUNE_SEARCH_SPACE,
            callbacks=[
                MLflowLoggerCallback(
                    tracking_uri=TUNE_CONFIG["mlflow_tracking_uri"],
                    experiment_name=experiment_name,
                    save_artifact=False,
                )
            ],
            num_samples=TUNE_CONFIG["num_trials"],
            scheduler=scheduler,
            search_alg=search_alg,
            verbose=2,
        )

        # Get best trial
        best_trial = tuner.get_best_trial("train_rmse", "min")

        return {
            "best_config": best_trial.config,
            "best_metrics": best_trial.last_result,
        }

    except Exception as e:
        raise AirflowException(f"Hyperparameter tuning failed: {e}")
