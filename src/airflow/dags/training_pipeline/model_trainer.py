import logging

from ray_provider.decorators import ray

from include.config.tune_config import RAY_TASK_CONFIG

logger = logging.getLogger(__name__)


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
