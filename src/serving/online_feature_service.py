from typing import Any, Dict

from feast import FeatureStore
from feast.errors import FeatureViewNotFoundException
from loguru import logger

from ray import serve


@serve.deployment(num_replicas=1)
class OnlineFeatureService:
    def __init__(self, repo_path: str = "./feature_stores"):
        self.store = FeatureStore(repo_path=repo_path)

    def get_online_features(self, user_id: int, product_id: int) -> Dict[str, Any]:
        """Get online features for prediction"""
        try:
            feature_vector = self.store.get_online_features(
                features=[
                    "user_features:activity_count",
                    "user_features:unique_products_viewed",
                    "product_features:price",
                    "product_features:category_code_level1",
                    "product_features:category_code_level2",
                ],
                entity_rows=[{"user_id": user_id, "product_id": product_id}],
            ).to_dict()

            return {"success": True, "features": feature_vector}

        except FeatureViewNotFoundException as e:
            logger.error(f"Feature view not found: {e}")
            return {"success": False, "error": str(e)}
        except Exception as e:
            logger.error(f"Error retrieving online features: {e}")
            return {"success": False, "error": str(e)}
