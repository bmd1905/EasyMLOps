from pathlib import Path

from dotenv import load_dotenv
from feast import FeatureStore
from features import product, product_features, streaming_features, user, user_features
from loguru import logger


def setup_feature_store():
    """Initialize and apply feature store configuration"""
    try:
        # Load environment variables
        load_dotenv()

        # Get the feature store directory
        repo_path = Path(__file__).parent

        # Initialize feature store
        store = FeatureStore(repo_path=repo_path)

        # Apply feature definitions
        store.apply(
            [user, product, user_features, product_features, streaming_features]
        )

        logger.info("Feature store setup completed successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to setup feature store: {e}")
        return False


if __name__ == "__main__":
    setup_feature_store()
