from typing import Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

import mlflow
from mlflow.tracking import MlflowClient


class PredictionService:
    def __init__(self, model_name: str, mlflow_uri: str = "http://localhost:5001"):
        """Initialize the prediction service"""
        self.model_name = model_name
        mlflow.set_tracking_uri(mlflow_uri)
        self.model, self.category_mappings = self._load_model_and_mappings()

        if not self.model or not self.category_mappings:
            raise RuntimeError("Failed to initialize prediction service")

    def _load_model_and_mappings(
        self,
    ) -> Tuple[Optional[mlflow.pyfunc.PyFuncModel], Optional[Dict]]:
        """Load the latest model version and its category mappings"""
        client = MlflowClient()

        try:
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

            model = mlflow.pyfunc.load_model(f"models:/{self.model_name}@current")
            category_mappings = mlflow.artifacts.load_dict(
                f"runs:/{run_id}/category_mappings.json"
            )

            return model, category_mappings

        except Exception as e:
            logger.error(f"Error loading model or mappings: {e}")
            return None, None

    def predict(self, features: List[Dict]) -> Dict:
        """Make predictions using the loaded model"""
        try:
            df = pd.DataFrame(features)

            # Encode categorical columns
            for col in ["brand", "category_code_level1", "category_code_level2"]:
                if col in df.columns:
                    mapping = self.category_mappings[col]
                    df[col] = df[col].map(mapping).fillna(-1)

            predictions = self.model.predict(df)

            return {
                "success": True,
                "predictions": predictions.tolist(),
                "encoded_features": df.to_dict(orient="records"),
            }

        except Exception as e:
            logger.error(f"Prediction error: {e}")
            return {"success": False, "error": str(e)}
