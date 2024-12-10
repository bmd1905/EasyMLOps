from loguru import logger
from ray.train import CheckpointConfig, FailureConfig, RunConfig, ScalingConfig
from ray.train.xgboost import XGBoostTrainer


def train_model(train_dataset, valid_dataset):
    """Train XGBoost model with Ray"""

    # Configure training with proper failure handling
    run_config = RunConfig(
        checkpoint_config=CheckpointConfig(
            checkpoint_frequency=1,
            num_to_keep=1,
        ),
        failure_config=FailureConfig(
            max_failures=0,  # Set to 0 since fail_fast is True
            fail_fast=True,
        ),
    )

    # Create trainer with conservative resource settings
    trainer = XGBoostTrainer(
        run_config=run_config,
        scaling_config=ScalingConfig(
            num_workers=1,  # Single worker for stability
            use_gpu=False,
            resources_per_worker={
                "CPU": 1,
                "memory": 2 * 1024 * 1024 * 1024,  # 2GB per worker
            },
        ),
        label_column="is_purchased",
        num_boost_round=20,
        params={
            "objective": "binary:logistic",
            "eval_metric": ["logloss", "error", "rmse", "mae", "auc", "pre", "map"],
            "tree_method": "hist",
            "max_depth": 6,
            "eta": 0.3,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
        },
        datasets={"train": train_dataset, "valid": valid_dataset},
    )

    try:
        result = trainer.fit()
        logger.info(f"Training completed. Metrics: {result.metrics}")
        return result
    except Exception as e:
        logger.error(f"Training failed: {e}")
        raise
