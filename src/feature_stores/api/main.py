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
    user_session: str = "040d0e0b-0a40-4d40-bdc9-c9252e877d9c"


@app.post("/features")
async def get_features(request: FeatureRequest):
    result = store.get_online_features(
        features=[
            "user_features:activity_count",
            "user_features:unique_products_viewed",
            "product_features:price",
            "product_features:category_code_level1",
            "product_features:category_code_level2",
        ],
        entity_rows=[{"user_id": request.user_id, "product_id": request.product_id}],
    ).to_dict()

    return result
