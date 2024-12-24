import pandas as pd
from loguru import logger

import mlflow
from mlflow.tracking import MlflowClient

# Configure logging
logger.add("logs/inference.log")

# Set MLflow tracking URI
mlflow.set_tracking_uri("http://localhost:5001")


def load_latest_model_and_mappings(model_name: str):
    """Load the latest model version and its category mappings"""
    client = MlflowClient()

    try:
        # Get latest model version using aliases instead of stages
        logger.info(f"Attempting to load latest version of model: {model_name}")
        latest_version = client.get_model_version_by_alias(model_name, "current")
        run_id = latest_version.run_id
        logger.info(
            f"Found model version: {latest_version.version} with run_id: {run_id}"
        )

        if not run_id:
            logger.error("No run_id found for the model version")
            return None, None

        # Load the model
        model = mlflow.pyfunc.load_model(f"models:/{model_name}@current")
        logger.info("Model loaded successfully")

        # Get category mappings from the same run
        try:
            # List artifacts to debug
            logger.info(f"Listing artifacts for run_id: {run_id}")
            artifacts = client.list_artifacts(run_id)
            logger.info(f"Available artifacts: {[art.path for art in artifacts]}")

            # Try to load the mappings
            category_mappings = mlflow.artifacts.load_dict(
                f"runs:/{run_id}/category_mappings.json"
            )
            logger.info("Category mappings loaded successfully")
            logger.debug(f"Mappings content: {category_mappings}")

            return model, category_mappings

        except Exception as e:
            logger.error(f"Error loading category mappings: {e}")
            return None, None

    except Exception as e:
        logger.error(f"Error loading model: {e}")
        return None, None


if __name__ == "__main__":
    # Model name
    model_name = "purchase_prediction_model"

    # Load model and mappings
    model, category_mappings = load_latest_model_and_mappings(model_name)

    if model is None or category_mappings is None:
        logger.error("Failed to load model or mappings")
    else:
        # Prepare inference data
        data = [
            {
                "brand": "sumsung",
                "price": 130.76,
                "event_weekday": 2,
                "category_code_level1": "electronics",
                "category_code_level2": "smartphone",
                "activity_count": 1,
            },
            {
                "brand": "video",
                "price": 130.76,
                "event_weekday": 2,
                "category_code_level1": "electronics",
                "category_code_level2": "smartphone",
                "activity_count": 1,
            },
        ]

        # Convert to DataFrame
        df = pd.DataFrame(data)
        logger.info(f"Input data shape: {df.shape}")

        # Encode categorical columns using saved mappings
        for col in ["brand", "category_code_level1", "category_code_level2"]:
            mapping = category_mappings[col]
            # Map values using the saved mapping, with -1 for unseen categories
            df[col] = df[col].map(mapping).fillna(-1)
            logger.info(f"Encoded column {col}")

        # Make predictions
        predictions = model.predict(df)
        logger.info(f"Predictions: {predictions}")

        # Show encoded features
        logger.info("\nEncoded Features:")
        logger.info(df.to_string())
