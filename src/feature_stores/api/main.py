from fastapi import FastAPI, HTTPException
from online_feature_service import OnlineFeatureService
from pydantic import BaseModel

app = FastAPI()
feature_service = OnlineFeatureService()


class FeatureRequest(BaseModel):
    user_id: int = 541312140
    product_id: int = 44600062


@app.post("/features")
async def get_features(request: FeatureRequest):
    result = feature_service.get_online_features(
        user_id=request.user_id, product_id=request.product_id
    )

    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])

    return result["features"]
