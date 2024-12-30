from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from feast import FeatureStore
from pydantic import BaseModel

store = FeatureStore(repo_path=".")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class FeatureRequest(BaseModel):
    user_id: int = 530834332
    product_id: int = 1005073


@app.post("/features")
async def get_features(request: FeatureRequest):
    result = store.get_online_features(
        features=[
            "streaming_features:activity_count",
            "streaming_features:event_weekday",
            "streaming_features:is_purchased",
            "streaming_features:brand",
            "product_features:price",
            "product_features:category_code_level1",
            "product_features:category_code_level2",
        ],
        entity_rows=[{"user_id": request.user_id, "product_id": request.product_id}],
    ).to_dict()

    return result


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
