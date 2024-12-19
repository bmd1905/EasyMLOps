from ray_provider.decorators import ray

from include.config.tune_config import RAY_TASK_CONFIG


@ray.task(config=RAY_TASK_CONFIG)
def train_final_model(data: dict, best_params: dict) -> dict:
    """Train final model with best parameters"""

    import json
    from datetime import datetime

    import pandas as pd
    import pyarrow.fs
    from loguru import logger

    import mlflow
    import ray
    from airflow.exceptions import AirflowException
    from include.config.tune_config import (
        MINIO_CONFIG,
        MODEL_NAME,
        TRAINING_CONFIG,
        TUNE_CONFIG,
        TUNE_SEARCH_SPACE,
        XGBOOST_PARAMS,
    )
    from mlflow.tracking.client import MlflowClient
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

        # Create MLflow experiment and start run
        experiment = mlflow.set_experiment(experiment_name)
        mlflow_run = mlflow.start_run(experiment_id=experiment.experiment_id)
        logger.info(f"Started MLflow run with ID: {mlflow_run.info.run_id}")

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

        # Log model directly to MLflow using the active run
        try:
            # Get the trained model from the checkpoint
            checkpoint = result.checkpoint
            logger.info(f"Loading model from checkpoint at: {checkpoint.path}")

            # Create S3 filesystem for loading
            fs = pyarrow.fs.S3FileSystem(**MINIO_CONFIG)

            # Load model directly from MinIO/S3
            model_path = f"{checkpoint.path}/model.ubj"
            logger.debug(f"Loading model from: {model_path}")

            # Read the UBJ file as bytes
            with fs.open_input_stream(model_path) as f:
                model_bytes = f.read()

            # Load the XGBoost model from bytes
            import xgboost as xgb

            trained_model = xgb.Booster()
            trained_model.load_model(bytearray(model_bytes))

            mlflow.xgboost.log_model(
                xgb_model=trained_model,
                artifact_path="model",
                registered_model_name=MODEL_NAME,
            )
            logger.info("Model logged to MLflow successfully")
            logger.debug(f"Checkpoint path: {checkpoint.path}")
        except Exception as e:
            logger.error(f"Error loading/logging model: {e}")
            logger.error(f"Checkpoint details: {checkpoint}")
            raise

        # Get the run ID from the active run
        mlflow_run_id = mlflow_run.info.run_id
        logger.info(f"MLflow run completed with ID: {mlflow_run_id}")

        # Register model in MLflow
        client = MlflowClient(tracking_uri=TUNE_CONFIG["mlflow_tracking_uri"])

        try:
            model_uri = f"runs:/{mlflow_run_id}/model"
            logger.debug(f"Model URI constructed: {model_uri}")

            # Create model version
            model_details = client.create_model_version(
                name=MODEL_NAME,
                source=model_uri,
                run_id=mlflow_run_id,
                description="XGBoost model for purchase prediction",
            )
            logger.info(f"Model version created: {model_details.version}")

            # Add tags to model version
            client.set_model_version_tag(
                name=MODEL_NAME,
                version=model_details.version,
                key="training_date",
                value=datetime.now().strftime("%Y-%m-%d"),
            )
            client.set_model_version_tag(
                name=MODEL_NAME,
                version=model_details.version,
                key="metrics",
                value=json.dumps(
                    {
                        "rmse": result.metrics.get("train-rmse"),
                        "auc": result.metrics.get("train-auc"),
                    }
                ),
            )

            # Set alias for the model version (using current instead of latest)
            client.set_registered_model_alias(
                name=MODEL_NAME, alias="current", version=model_details.version
            )

            # Transition to staging
            client.transition_model_version_stage(
                name=MODEL_NAME, version=model_details.version, stage="Staging"
            )
            logger.info(
                f"Model version {model_details.version} registered and moved to staging"
            )
        except Exception as e:
            logger.error(f"Error during model registration: {e}")
            raise
        finally:
            # End the MLflow run
            mlflow.end_run()

        # Transform metrics to match expected format
        metrics = {
            "train_rmse": result.metrics.get("train-rmse", float("inf")),
            "train_logloss": result.metrics.get("train-logloss", float("inf")),
            "train_error": result.metrics.get("train-error", float("inf")),
            "train_auc": result.metrics.get("train-auc", 0.0),
            "train_mae": result.metrics.get("train-mae", float("inf")),
            "mlflow_run_id": mlflow_run_id,
        }

        return {
            "metrics": metrics,
            "checkpoint_path": result.checkpoint.path,
            "best_params": best_params or {"best_config": model_params},
            "mlflow_model_uri": f"models:/{MODEL_NAME}/Staging",
        }

    except Exception as e:
        # Ensure MLflow run is ended even if there's an error
        try:
            mlflow.end_run()  # noqa: E722
        except Exception:  # noqa: E722
            pass
        raise AirflowException(f"Final model training failed: {e}")
