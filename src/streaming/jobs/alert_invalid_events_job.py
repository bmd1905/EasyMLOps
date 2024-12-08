import json
import os
from datetime import datetime
from typing import Any, Dict

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment

from ..connectors.sources.kafka_source import build_source
from ..jobs.base import FlinkJob


class AlertInvalidEventsJob(FlinkJob):
    def __init__(self):
        self.jars_path = f"{os.getcwd()}/src/streaming/connectors/config/jars/"
        self.input_topic = os.getenv("KAFKA_INVALID_TOPIC", "invalidated-events-topic")
        self.group_id = os.getenv("KAFKA_GROUP_ID", "flink-alert-group")
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    @property
    def job_name(self) -> str:
        return "alert_invalid_events"

    def process_invalid_event(self, event: str) -> Dict[str, Any]:
        """Process invalid event and format alert message"""
        try:
            event_data = json.loads(event)
            return {
                "timestamp": datetime.now().isoformat(),
                "error_type": event_data.get("error_type", "UNKNOWN"),
                "error_message": event_data.get("error_message", "No error message"),
                # "raw_event": event_data,
                "severity": "HIGH"
                if "schema" in event_data.get("error_type", "").lower()
                else "MEDIUM",
            }
        except Exception as e:
            return {
                "timestamp": datetime.now().isoformat(),
                "error_type": "PROCESSING_ERROR",
                "error_message": str(e),
                "raw_event": event,
                "severity": "LOW",
            }

    def alert_handler(self, alert: Dict[str, Any]) -> None:
        """Handle alerts - you can customize this to send to different destinations"""
        # For now, just print to console
        print(f"⚠️ ALERT: {json.dumps(alert, indent=2)}")
        # TODO: Add more alert destinations (e.g., Slack, email, etc.)

    def create_pipeline(self, env: StreamExecutionEnvironment):
        # Add required JARs
        env.add_jars(
            f"file://{self.jars_path}/flink-connector-kafka-1.17.1.jar",
            f"file://{self.jars_path}/kafka-clients-3.4.0.jar",
        )

        # Create source
        source = build_source(
            topics=self.input_topic,
            group_id=self.group_id,
            bootstrap_servers=self.bootstrap_servers,
        )

        # Build pipeline
        stream = env.from_source(
            source, WatermarkStrategy.no_watermarks(), "Invalid Events Alert Job"
        )

        # Process invalid events and generate alerts
        (
            stream.map(self.process_invalid_event, output_type=Types.STRING()).map(
                self.alert_handler
            )
        )
