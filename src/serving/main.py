from typing import List

from fastapi import FastAPI, HTTPException
from online_feature_service import OnlineFeatureService
from prediction_service import PredictionService
from pydantic import BaseModel

app = FastAPI()
feature_service = OnlineFeatureService(repo_path="../feature_stores")
prediction_service = PredictionService(model_name="purchase_prediction_model")


class PredictionRequest(BaseModel):
    user_id: int = 530834332
    product_id: int = 1005073
    user_session: str = "040d0e0b-0a40-4d40-bdc9-c9252e877d9c"


FEATURE_COLUMNS = [
    "user_session",
    "activity_count",
    "unique_products_viewed",
    "price",
    "category_code_level1",
    "category_code_level2",
]


@app.post("/predict")
async def predict(requests: List[PredictionRequest]):
    features = []

    # Get features from online store for each request
    for request in requests:
        # Get online features
        feature_result = feature_service.get_online_features(
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
        {key: feature[key] for key in FEATURE_COLUMNS if key in feature}
        for feature in features
    ]

    # Get predictions using the combined features
    result = prediction_service.predict(features)

    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])

    return {
        "predictions": result["predictions"],
        "encoded_features": result["encoded_features"],
    }
