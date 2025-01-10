from datetime import timedelta

from data_sources import validated_events_stream
from feast import Field
from feast.stream_feature_view import stream_feature_view
from feast.types import Float32, Int64, String
from pyspark.sql import DataFrame


@stream_feature_view(
    entities=[],
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
