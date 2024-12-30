from datetime import timedelta

from data_sources import product_source, user_source, validated_events_stream
from feast import Entity, FeatureView, Field
from feast.stream_feature_view import stream_feature_view
from feast.types import Float32, Int64, String
from pyspark.sql import DataFrame

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
        Field(name="category_code_level1", dtype=String),
        Field(name="category_code_level2", dtype=String),
    ],
    source=product_source,
    online=True,
)


@stream_feature_view(
    entities=[user, product],
    ttl=timedelta(days=1),
    mode="spark",
    schema=[
        Field(name="price", dtype=Float32),
        Field(name="category_code_level1", dtype=String),
        Field(name="category_code_level2", dtype=String),
        Field(name="brand", dtype=String),
        Field(name="activity_count", dtype=Int64),
        Field(name="event_weekday", dtype=Float32),
        Field(name="is_purchased", dtype=Int64),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=validated_events_stream,
)
def streaming_features(df: DataFrame):
    """Process streaming features using Spark SQL."""
    return df
