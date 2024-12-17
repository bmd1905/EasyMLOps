from feast import FeatureStore
from loguru import logger


def test_feature_store():
    """Test feature store functionality"""
    try:
        # Initialize feature store
        store = FeatureStore(repo_path=".")

        # Test online feature retrieval
        features = store.get_online_features(
            features=[
                "user_features:activity_count",
                "user_features:unique_products_viewed",
                "product_features:price",
                "product_features:category_code_level1",
                "product_features:category_code_level2",
            ],
            entity_rows=[{"user_id": 123, "product_id": 456}],
        ).to_dict()

        logger.info("Retrieved features:")
        logger.info(features)

    except Exception as e:
        logger.error(f"Test failed: {e}")


if __name__ == "__main__":
    test_feature_store()
