from typing import Dict, List

import pandas as pd
import xgboost as xgb
from loguru import logger
from opentelemetry import trace
from opentelemetry.trace import SpanKind

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
        self.tracer = trace.get_tracer(__name__)
        self._load_model()

    def _load_model(self):
        try:
            with self.tracer.start_as_current_span("load_model") as span:
                mlflow.set_tracking_uri(self.mlflow_uri)
                client = MlflowClient()

                # Get model version by alias instead of latest version
                model_version = client.get_model_version_by_alias(
                    self.model_name, "current"
                )
                span.set_attribute("model.version", model_version.version)
                run_id = model_version.run_id

                # Load model using the model URI
                model_uri = f"models:/{self.model_name}@current"
                self.model = mlflow.xgboost.load_model(model_uri)

                # Load category mappings using the run ID
                try:
                    # Use mlflow.artifacts.load_dict with the correct artifact path
                    self.category_mappings = mlflow.artifacts.load_dict(
                        f"runs:/{run_id}/category_mappings.json"
                    )
                    logger.info(
                        f"Successfully loaded model {self.model_name} and category mappings"
                    )
                except Exception as e:
                    logger.error(f"Failed to load category mappings: {str(e)}")
                    raise

        except Exception as e:
            logger.error(f"Failed to load model: {str(e)}")
            raise

    async def __call__(self, features: List[Dict]) -> Dict:
        try:
            with self.tracer.start_as_current_span(
                "predict", kind=SpanKind.SERVER
            ) as span:
                span.set_attribute("feature_count", len(features))

                # Convert to DataFrame
                with self.tracer.start_span("prepare_features") as prep_span:  # noqa: F841
                    df = pd.DataFrame(features)

                    # Encode categorical columns
                    categorical_cols = [
                        "brand",
                        "category_code_level1",
                        "category_code_level2",
                    ]
                    for col in categorical_cols:
                        if col in df.columns:
                            mapping = self.category_mappings.get(col, {})
                            df[col] = df[col].map(mapping).fillna(-1).astype("int64")

                    # Create DMatrix with categorical features enabled
                    dmatrix = xgb.DMatrix(
                        df,
                        enable_categorical=True,
                        feature_types=[
                            "c" if col in categorical_cols else "q"
                            for col in df.columns
                        ],
                    )

                # Make predictions
                with self.tracer.start_span("model_predict") as predict_span:
                    predictions = self.model.predict(dmatrix)
                    predict_span.set_attribute("prediction_count", len(predictions))

                return {"predictions": predictions.tolist(), "success": True}

        except Exception as e:
            logger.error(f"Prediction failed: {str(e)}")
            return {"predictions": [], "success": False, "error": str(e)}
