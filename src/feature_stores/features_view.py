from datetime import timedelta

from data_sources import validated_events_stream
from entities import product, user
from feast import Field
from feast.stream_feature_view import stream_feature_view
from feast.types import Float32, Int64, String
from pyspark.sql import DataFrame


@stream_feature_view(
    entities=[user, product],
    ttl=timedelta(days=1),
    mode="spark",
    schema=[
        Field(name="user_id", dtype=Int64),
        Field(name="product_id", dtype=Int64),
        Field(name="event_timestamp", dtype=String),
        Field(name="price", dtype=Float32),
        Field(name="category_code_level1", dtype=String),
        Field(name="category_code_level2", dtype=String),
        Field(name="brand", dtype=String),
        Field(name="activity_count", dtype=Int64),
        Field(name="event_weekday", dtype=Float32),
        Field(name="is_purchased", dtype=Int64),
        Field(name="user_session", dtype=String),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=validated_events_stream,
)
def streaming_features(df: DataFrame):
    return df
