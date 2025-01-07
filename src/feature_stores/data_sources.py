from datetime import timedelta

from feast import KafkaSource
from feast.data_format import JsonFormat
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)

# Source for user features
user_source = PostgreSQLSource(
    name="user_features_source",
    query="""
    SELECT
        user_id,
        event_count as activity_count,
        unique_products_viewed,
        session_start as event_timestamp,
        user_session
    FROM dwh.vw_user_session_summary
    """,
    timestamp_field="event_timestamp",
)

# Source for product features
product_source = PostgreSQLSource(
    name="product_features_source",
    query="""
    SELECT DISTINCT
        f.product_id,
        p.price,
        SPLIT_PART(c.category_code, '.', 1) as category_code_level1,
        SPLIT_PART(c.category_code, '.', 2) as category_code_level2,
        f.event_timestamp
    FROM dwh.fact_events f
    JOIN dwh.dim_product p ON f.product_id = p.product_id
    JOIN dwh.dim_category c ON f.category_id = c.category_id
    WHERE c.category_code IS NOT NULL
        AND f.event_type IN ('cart', 'purchase')
    """,
    timestamp_field="event_timestamp",
)


# Batch source for validated events (historical data)
validated_events_batch = PostgreSQLSource(
    name="validated_events_batch",
    query="SELECT * FROM dwh.vw_ml_purchase_prediction",
    timestamp_field="event_timestamp",
)

# Stream source for real-time events from Kafka
validated_events_stream = KafkaSource(
    name="validated_events_stream",
    kafka_bootstrap_servers="localhost:9092",
    topic="model.features.ready",
    timestamp_field="event_timestamp",
    batch_source=validated_events_batch,
    message_format=JsonFormat(
        schema_json="""
        {
            "type": "record",
            "name": "feature_event",
            "fields": [
                {"name": "event_timestamp", "type": "string"},
                {"name": "user_id", "type": "int"},
                {"name": "product_id", "type": "int"},
                {"name": "user_session", "type": "string"},
                {"name": "event_type", "type": "string"},
                {"name": "category_code", "type": "string"},
                {"name": "price", "type": "float"},
                {"name": "brand", "type": "string"},
                {"name": "category_code_level1", "type": "string"},
                {"name": "category_code_level2", "type": "string"},
                {"name": "processed_at", "type": "string"}
            ]
        }
        """
    ),
    description="Stream of feature events from Kafka",
    tags={"team": "ml_team"},
    watermark_delay_threshold=timedelta(minutes=1),
)
