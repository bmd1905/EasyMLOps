from typing import Dict, List

import pandas as pd
import xgboost as xgb
from loguru import logger

import mlflow
from mlflow.tracking import MlflowClient
from ray import serve


@serve.deployment(num_replicas=1)
class PredictionService:
    def __init__(self, model_name: str, mlflow_uri: str):
        self.model_name = model_name
        self.mlflow_uri = mlflow_uri
        self.model = None
        self.category_mappings = None
        self._load_model()

    def _load_model(self):
        try:
            mlflow.set_tracking_uri(self.mlflow_uri)
            client = MlflowClient()

            # Get latest model version
            latest_version = client.get_latest_versions(self.model_name)[0]

            # Load model and mappings
            self.model = mlflow.xgboost.load_model(latest_version.source)
            self.category_mappings = mlflow.artifacts.load_dict(
                f"runs/{latest_version.run_id}/category_mappings.json"
            )

            logger.info(f"Successfully loaded model {self.model_name}")
        except Exception as e:
            logger.error(f"Failed to load model: {str(e)}")
            raise

    async def __call__(self, features: List[Dict]) -> Dict:
        try:
            # Convert to DataFrame
            df = pd.DataFrame(features)

            # Encode categorical columns
            categorical_cols = ["brand", "category_code_level1", "category_code_level2"]
            for col in categorical_cols:
                if col in df.columns:
                    mapping = self.category_mappings.get(col, {})
                    df[col] = df[col].map(mapping).fillna(-1).astype("int64")

            # Create DMatrix with categorical features enabled
            dmatrix = xgb.DMatrix(
                df,
                enable_categorical=True,
                feature_types=[
                    "c" if col in categorical_cols else "q" for col in df.columns
                ],
            )

            # Make predictions
            predictions = self.model.predict(dmatrix)

            return {"predictions": predictions.tolist(), "success": True}

        except Exception as e:
            logger.error(f"Prediction failed: {str(e)}")
            return {"predictions": [], "success": False, "error": str(e)}
