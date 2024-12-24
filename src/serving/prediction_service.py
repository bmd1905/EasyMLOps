from typing import Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

import mlflow
from mlflow.tracking import MlflowClient
from ray import serve


@serve.deployment(
    ray_actor_options={"num_cpus": 2},
    autoscaling_config={"min_replicas": 1, "max_replicas": 5},
)
class PredictionService:
    def __init__(
        self, model_name: str, mlflow_uri: str = "http://tracking_server:5000"
    ):
        """Initialize the prediction service"""
        logger.info(f"Initializing PredictionService for model: {model_name}")
        self.model_name = model_name

        # Retry logic for MLflow connection
        max_retries = 3
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                mlflow.set_tracking_uri(mlflow_uri)
                # Test connection
                MlflowClient().get_registered_model(self.model_name)
                logger.info(f"Successfully connected to MLflow at {mlflow_uri}")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(
                        f"Failed to connect to MLflow after {max_retries} attempts: {e}"
                    )
                    raise RuntimeError(
                        f"Could not connect to MLflow server at {mlflow_uri}"
                    )
                logger.warning(
                    f"Attempt {attempt + 1} failed, retrying in {retry_delay}s: {e}"
                )
                import time

                time.sleep(retry_delay)

        self.model, self.category_mappings = self._load_model_and_mappings()

        if not self.model or not self.category_mappings:
            logger.error("Failed to initialize prediction service")
            raise RuntimeError("Failed to initialize prediction service")

        logger.info("PredictionService initialized successfully")

    def _load_model_and_mappings(
        self,
    ) -> Tuple[Optional[mlflow.pyfunc.PyFuncModel], Optional[Dict]]:
        """Load the latest model version and its category mappings"""
        client = MlflowClient()

        try:
            logger.info(
                f"Attempting to load latest model version for {self.model_name}"
            )
            latest_version = client.get_model_version_by_alias(
                self.model_name, "current"
            )
            run_id = latest_version.run_id
            logger.info(
                f"Found model version: {latest_version.version} with run_id: {run_id}"
            )

            if not run_id:
                logger.error("No run_id found for the model version")
                return None, None

            logger.info(f"Loading model from: models:/{self.model_name}@current")
            model = mlflow.pyfunc.load_model(f"models:/{self.model_name}@current")

            logger.info(
                f"Loading category mappings from: runs:/{run_id}/category_mappings.json"
            )
            category_mappings = mlflow.artifacts.load_dict(
                f"runs:/{run_id}/category_mappings.json"
            )

            logger.info("Model and category mappings loaded successfully")
            return model, category_mappings

        except Exception as e:
            logger.error(f"Error loading model or mappings: {e}")
            return None, None

    def predict(self, features: List[Dict]) -> Dict:
        """Make predictions using the loaded model"""
        try:
            logger.info(f"Received prediction request for {len(features)} features")
            df = pd.DataFrame(features)
            logger.debug(f"Input DataFrame shape: {df.shape}")

            # Encode categorical columns
            logger.info("Encoding categorical columns")
            for col in ["brand", "category_code_level1", "category_code_level2"]:
                if col in df.columns:
                    mapping = self.category_mappings[col]
                    df[col] = df[col].map(mapping).fillna(-1)
                    logger.debug(f"Encoded column {col}")

            logger.info("Running model prediction")
            predictions = self.model.predict(df)
            logger.info(f"Generated {len(predictions)} predictions")

            return {
                "success": True,
                "predictions": predictions.tolist(),
                "encoded_features": df.to_dict(orient="records"),
            }

        except Exception as e:
            logger.error(f"Prediction error: {e}")
            return {"success": False, "error": str(e)}
