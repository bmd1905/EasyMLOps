from pathlib import Path

from dotenv import load_dotenv
from entities import product, user
from feast import FeatureStore
from features_view import streaming_features
from loguru import logger
from setup_db import init_database


def setup_feature_store():
    """Initialize and apply feature store configuration"""
    try:
        # Load environment variables
        load_dotenv()

        # Initialize database
        init_database()

        # Get the feature store directory
        repo_path = Path(__file__).parent

        # Initialize feature store
        store = FeatureStore(repo_path=repo_path)

        # Apply feature definitions including entities
        store.apply(
            [
                user,  # Register user entity
                product,  # Register product entity
                streaming_features,
            ]
        )

        logger.info("Feature store setup completed successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to setup feature store: {e}")
        return False


if __name__ == "__main__":
    setup_feature_store()
