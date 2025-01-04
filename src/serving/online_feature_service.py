from typing import Any, Dict

import requests
from loguru import logger
from ray import serve
from opentelemetry import trace
from opentelemetry.propagate import inject
from opentelemetry.trace import SpanKind


@serve.deployment(num_replicas=1)
class OnlineFeatureService:
    def __init__(self, feature_retrieval_url: str):
        self.feature_retrieval_url = feature_retrieval_url
        self.tracer = trace.get_tracer(__name__)

    def __call__(self, user_id: int, product_id: int) -> Dict[str, Any]:
        """Get online features for prediction"""
        try:
            with self.tracer.start_as_current_span(
                "get_online_features", kind=SpanKind.CLIENT
            ) as span:
                # Add attributes to span
                span.set_attributes({"user_id": user_id, "product_id": product_id})

                logger.info(
                    f"Retrieving online features for user_id: {user_id}, product_id: {product_id}"
                )

                # Prepare headers with trace context
                headers = {}
                inject(headers)  # This injects the current trace context into headers

                # Make request to the feature store with trace context
                response = requests.post(
                    f"{self.feature_retrieval_url}/features",
                    json={"user_id": user_id, "product_id": product_id},
                    headers=headers,
                )
                feature_vector = response.json()

                logger.info(
                    f"Successfully retrieved features for user_id: {user_id}, product_id: {product_id}"
                )
                return {"success": True, "features": feature_vector}

        except Exception as e:
            logger.error(f"Error retrieving online features: {e}")
            return {"success": False, "error": str(e)}
