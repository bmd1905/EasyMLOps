from datetime import datetime, timedelta

import redis
from dotenv import load_dotenv
from feast import FeatureStore
from loguru import logger


def materialize_features():
    """Materialize features from offline store to online store"""
    try:
        # Load environment variables
        load_dotenv()

        # Initialize feature store
        store = FeatureStore(repo_path=".")

        # Calculate time range
        end_date = datetime(2019, 10, 10)
        start_date = end_date - timedelta(days=30)

        # Materialize features using feature view names
        store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=["validated_events_stream"],
        )

        logger.info("Successfully materialized features")
        return True

    except redis.ConnectionError as e:
        logger.error(f"Redis connection error: {e}")
        logger.info("Please ensure Redis is running and accessible")
        return False
    except Exception as e:
        logger.error(f"Failed to materialize features: {e}")
        return False


if __name__ == "__main__":
    materialize_features()
