import json
import os
from typing import List

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from online_feature_service import OnlineFeatureService
from prediction_service import PredictionService
from pydantic import BaseModel

import ray
from ray import serve

app = FastAPI()


working_dir = os.path.dirname(os.path.abspath(__file__))

ray.init(
    address="auto",
    namespace="serving",
    runtime_env={"working_dir": working_dir},
    log_to_driver=True,
)


@serve.deployment(num_replicas=1)
@serve.ingress(app)
class APIIngress:
    class PredictionRequest(BaseModel):
        user_id: int = 530834332
        product_id: int = 1005073
        user_session: str = "040d0e0b-0a40-4d40-bdc9-c9252e877d9c"

    def __init__(
        self,
        feature_service: OnlineFeatureService,
        prediction_service: PredictionService,
    ):
        self._feature_service = feature_service
        self._prediction_service = prediction_service
        self._FEATURE_COLUMNS = [
            "user_session",
            "activity_count",
            "unique_products_viewed",
            "price",
            "category_code_level1",
            "category_code_level2",
        ]

    @app.post("/predict")
    async def predict(self, requests: List[PredictionRequest]):
        features = []

        # Get features from online store for each request
        for request in requests:
            # Get online features
            feature_result = await self._feature_service.remote(
                user_id=request.user_id, product_id=request.product_id
            )

            if not feature_result["success"]:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to get features for user_id={request.user_id}, product_id={request.product_id}: {feature_result['error']}",
                )

            # Convert feature lists to single values since we're getting only one row
            feature_dict = {}
            for key, value in feature_result["features"].items():
                # Online store returns lists, we need the first value
                feature_dict[key] = value[0] if isinstance(value, list) else value

            features.append(feature_dict)

        # Filter features to only include the ones we need
        features = [
            {key: feature[key] for key in self._FEATURE_COLUMNS if key in feature}
            for feature in features
        ]

        # Get predictions using the combined features
        result = await self._prediction_service.remote(features)

        # Load to json
        result_json = json.dumps(result)

        return Response(
            content=result_json,
            media_type="application/json",
        )


feature_service = OnlineFeatureService.bind(
    feature_retrieval_url="http://feature-retrieval:8001"
)
prediction_service = PredictionService.bind(
    model_name="purchase_prediction_model", mlflow_uri="http://mlflow_server:5000"
)
entrypoint = APIIngress.bind(feature_service, prediction_service)

serve.start(http_options={"host": "0.0.0.0", "port": 8000})
