from datetime import timedelta

from feast import KafkaSource
from feast.data_format import JsonFormat
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)

# Batch source for validated events (historical data)
validated_events_batch = PostgreSQLSource(
    name="validated_events_batch",
    query="SELECT * FROM feature_store.ml_purchase_prediction WHERE 1=1",
    timestamp_field="event_timestamp",
)

# Stream source for real-time events from Kafka
validated_events_stream = KafkaSource(
    name="validated_events_stream",
    kafka_bootstrap_servers="localhost:9092",
    topic="tracking.user_behavior.validated",
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
