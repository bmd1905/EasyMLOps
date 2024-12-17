from datetime import timedelta
from feast import Entity, FeatureView, Field
from feast.types import Float32, Int64
from data_sources import user_source, product_source

# Define entities
user = Entity(name="user", join_keys=["user_id"], description="user identifier")
product = Entity(
    name="product", join_keys=["product_id"], description="product identifier"
)

# Feature view for user behavior
user_features = FeatureView(
    name="user_features",
    entities=[user],
    ttl=timedelta(days=1),
    schema=[
        Field(name="activity_count", dtype=Int64),
        Field(name="unique_products_viewed", dtype=Int64),
    ],
    source=user_source,
    online=True,
)

# Feature view for product features
product_features = FeatureView(
    name="product_features",
    entities=[product],
    ttl=timedelta(days=1),
    schema=[
        Field(name="price", dtype=Float32),
        Field(name="category_code_level1", dtype=Int64),
        Field(name="category_code_level2", dtype=Int64),
    ],
    source=product_source,
    online=True,
)
