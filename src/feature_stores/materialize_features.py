from datetime import datetime, timedelta
from feast import FeatureStore
from loguru import logger
import redis
import os
from dotenv import load_dotenv


def materialize_features():
    """Materialize features from offline store to online store"""
    try:
        # Load environment variables
        load_dotenv()

        # Test Redis connection first
        redis_host, redis_port = os.getenv("REDIS_CONNECTION_STRING").split(":")
        redis_client = redis.Redis(
            host=redis_host, port=int(redis_port), decode_responses=True
        )
        redis_client.ping()  # Test connection

        # Initialize feature store
        store = FeatureStore(repo_path=".")

        # Calculate time range
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=1)  # noqa: F841

        # Materialize features
        store.materialize_incremental(
            end_date=end_date,
            feature_views=store.list_feature_views(),
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
