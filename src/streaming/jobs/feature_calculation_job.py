import json
import os
from datetime import datetime

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment

from ..connectors.sinks.kafka_sink import build_sink
from ..connectors.sources.kafka_source import build_source
from ..jobs.base import FlinkJob


def parse_and_validate_event(event: str) -> str:
    """Parse and validate the event data."""
    try:
        data = json.loads(event)
        if data.get("valid") != "VALID":
            print(f"Invalid event: {data.get('error_message')}")
            return None

        payload = data.get("payload", {})
        if not all(
            k in payload
            for k in ["event_time", "user_id", "product_id", "user_session"]
        ):
            print(f"Missing required fields in payload: {payload}")
            return None

        # Extract fields from payload and keep category_code for feature calculation
        extracted = {
            "event_timestamp": payload["event_time"],
            "user_id": payload["user_id"],
            "product_id": payload["product_id"],
            "user_session": payload["user_session"],
            "event_type": payload.get("event_type"),
            "category_code": payload.get(
                "category_code"
            ),  # Keep the full category_code
            "price": payload.get("price", 0.0),
            "brand": payload.get("brand"),
        }
        return json.dumps(extracted)
    except Exception as e:
        print(f"Error parsing event: {str(e)}")
        return None


def calculate_features(event: str) -> str:
    """Calculate features from the event data."""
    try:
        data = json.loads(event)

        # Extract category levels with null check
        category_code = data.get("category_code")
        if category_code:  # Only split if category_code exists
            category_parts = category_code.split(".")
            category_code_level1 = (
                category_parts[0] if len(category_parts) > 0 else None
            )
            category_code_level2 = (
                category_parts[1] if len(category_parts) > 1 else None
            )
        else:
            category_code_level1 = None
            category_code_level2 = None

        # Add the calculated features
        data.update(
            {
                "category_code_level1": category_code_level1,
                "category_code_level2": category_code_level2,
                "processed_at": datetime.utcnow().isoformat(),
            }
        )

        return json.dumps(data)
    except Exception as e:
        print(f"Error calculating features: {str(e)}")
        return None


class FeatureCalculationJob(FlinkJob):
    def __init__(self):
        self.jars_path = f"{os.getcwd()}/src/streaming/connectors/config/jars/"
        self.input_topic = os.getenv("KAFKA_VALID_TOPIC", "validated-events-topic")
        self.output_topic = os.getenv("KAFKA_FEATURES_TOPIC", "feature-events-topic")
        self.group_id = os.getenv("KAFKA_GROUP_ID", "flink-feature-group")
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    @property
    def job_name(self) -> str:
        return "feature_calculation"

    def create_pipeline(self, env: StreamExecutionEnvironment):
        # Add required JARs
        env.add_jars(
            f"file://{self.jars_path}/flink-connector-kafka-1.17.1.jar",
            f"file://{self.jars_path}/kafka-clients-3.4.0.jar",
        )

        # Create source and sink
        source = build_source(
            topics=self.input_topic,
            group_id=self.group_id,
            bootstrap_servers=self.bootstrap_servers,
        )
        sink = build_sink(self.output_topic, self.bootstrap_servers)

        # Create the processing pipeline
        stream = env.from_source(
            source, WatermarkStrategy.no_watermarks(), "Feature Calculation Source"
        )

        # Parse and validate events
        valid_stream = stream.map(
            parse_and_validate_event, output_type=Types.STRING()
        ).filter(lambda x: x is not None)

        # Calculate features
        feature_stream = valid_stream.map(
            calculate_features, output_type=Types.STRING()
        ).filter(lambda x: x is not None)

        # Sink the results
        feature_stream.sink_to(sink)
