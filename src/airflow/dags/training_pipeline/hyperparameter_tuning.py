import logging

from ray_provider.decorators import ray

from include.config.tune_config import RAY_TASK_CONFIG

logger = logging.getLogger(__name__)


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
