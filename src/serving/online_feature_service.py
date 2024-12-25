from typing import Any, Dict

import requests
from loguru import logger

from ray import serve


@serve.deployment(num_replicas=1)
class OnlineFeatureService:
    def __init__(self, feature_retrieval_url: str):
        self.feature_retrieval_url = feature_retrieval_url

    def __call__(self, user_id: int, product_id: int) -> Dict[str, Any]:
        """Get online features for prediction"""
        try:
            logger.info(
                f"Retrieving online features for user_id: {user_id}, product_id: {product_id}"
            )

            # make a request to the feature store
            response = requests.post(
                f"{self.feature_retrieval_url}/features",
                json={"user_id": user_id, "product_id": product_id},
            )
            feature_vector = response.json()

            logger.info(
                f"Successfully retrieved features for user_id: {user_id}, product_id: {product_id}"
            )
            return {"success": True, "features": feature_vector}

        except Exception as e:
            logger.error(f"Error retrieving online features: {e}")
            return {"success": False, "error": str(e)}
