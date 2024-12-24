import json
import os
import time
from datetime import datetime
from threading import Lock
from typing import Any, Dict, Tuple

from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

from ..connectors.sinks.kafka_sink import build_sink
from ..connectors.sources.kafka_source import build_source
from ..jobs.base import FlinkJob


class RequestCounter:
    def __init__(self):
        self.count = 0
        self.last_log_time = time.time()
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.count += 1
            current_time = time.time()
            if current_time - self.last_log_time >= 1.0:
                print(f"Processed {self.count} records in the last second")
                self.count = 0
                self.last_log_time = current_time


request_counter = RequestCounter()


def validate_field_type(value: Any, field_def: Dict) -> bool:
    """Validate a field's type against its schema definition."""
    field_type = (
        field_def.get("name")
        if field_def.get("name", "").startswith("io.debezium.time")
        else field_def["type"]
    )

    # Handle null values for optional fields
    if value is None:
        return field_def.get("optional", False)

    type_validators = {
        "string": lambda v: isinstance(v, str),
        "int64": lambda v: isinstance(v, int),
        "double": lambda v: isinstance(v, (float, int)),
        "boolean": lambda v: isinstance(v, bool),
        "io.debezium.time.MicroTimestamp": lambda v: isinstance(v, int) and v >= 0,
        "io.debezium.time.Timestamp": lambda v: isinstance(v, int) and v >= 0,
        "io.debezium.time.ZonedTimestamp": lambda v: isinstance(v, str)
        and _is_valid_timestamp(v),
    }

    validator = type_validators.get(field_type)
    return validator and validator(value)


def _is_valid_timestamp(value: str) -> bool:
    """Check if a string is a valid timestamp."""
    timestamp_formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
    ]

    for fmt in timestamp_formats:
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return False


def validate_field_value(value: Any, field_def: Dict) -> Tuple[bool, str]:
    """Validate a field's value constraints."""
    field_name = field_def["field"]

    if field_name == "price" and value is not None:
        return (value > 0, "Price should be greater than 0")

    if field_name == "event_type" and value is not None:
        valid_types = ["view", "cart", "purchase", "remove_from_cart"]
        return (
            value in valid_types,
            f"Event type should be one of: {', '.join(valid_types)}",
        )

    return True, None


def validate_schema(record: str) -> str:
    """Validate a record against its schema."""
    request_counter.increment()

    try:
        record_dict = json.loads(record) if isinstance(record, str) else record

        if "metadata" not in record_dict:
            record_dict["metadata"] = {}

        schema = record_dict["schema"]
        payload = record_dict["payload"]

        # Validate each field
        for field in schema["fields"]:
            field_name = field["field"]

            # Check if required field is present
            if field_name not in payload and not field.get("optional", False):
                record_dict.update(
                    {
                        "valid": "INVALID",
                        "error_message": f"Field {field_name} is required",
                        "error_type": "SCHEMA_VALIDATION_ERROR",
                    }
                )
                return json.dumps(record_dict)

            # Skip validation for missing optional fields
            if field_name not in payload:
                continue

            # Validate type
            if not validate_field_type(payload[field_name], field):
                record_dict.update(
                    {
                        "valid": "INVALID",
                        "error_message": f"Field {field_name} has invalid type",
                        "error_type": "SCHEMA_VALIDATION_ERROR",
                    }
                )
                return json.dumps(record_dict)

            # Validate value constraints
            valid, error_msg = validate_field_value(payload[field_name], field)
            if not valid:
                record_dict.update(
                    {
                        "valid": "INVALID",
                        "error_message": error_msg,
                        "error_type": "SCHEMA_VALIDATION_ERROR",
                    }
                )
                return json.dumps(record_dict)

        # All validations passed
        record_dict.update(
            {"valid": "VALID", "error_message": None, "error_type": None}
        )

        record_dict["metadata"]["processed_at"] = datetime.utcnow().isoformat()
        return json.dumps(record_dict)

    except Exception as e:
        data = {
            "valid": "INVALID",
            "error_message": f"Processing error: {str(e)}",
            "error_type": "PROCESSING_ERROR",
            "raw_data": record,
            "metadata": {"processed_at": datetime.utcnow().isoformat()},
        }
        return json.dumps(data)


class SchemaValidationJob(FlinkJob):
    def __init__(self):
        self.jars_path = f"{os.getcwd()}/src/streaming/connectors/config/jars/"
        self.input_topics = os.getenv("KAFKA_INPUT_TOPICS", "raw-events-topic").split(
            ","
        )
        self.group_id = os.getenv("KAFKA_GROUP_ID", "flink-group")
        self.valid_topic = os.getenv("KAFKA_VALID_TOPIC", "validated-events-topic")
        self.invalid_topic = os.getenv(
            "KAFKA_INVALID_TOPIC", "invalidated-events-topic"
        )
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    @property
    def job_name(self) -> str:
        return "schema_validation"

    def create_pipeline(self, env: StreamExecutionEnvironment):
        env.set_parallelism(4)
        env.add_jars(
            f"file://{self.jars_path}/flink-connector-kafka-1.17.1.jar",
            f"file://{self.jars_path}/kafka-clients-3.4.0.jar",
        )

        # Create sources and sinks
        sources = [
            build_source(
                topic.strip(), f"{self.group_id}-{topic}", self.bootstrap_servers
            )
            for topic in self.input_topics
        ]

        valid_sink = build_sink(self.valid_topic, self.bootstrap_servers)
        invalid_sink = build_sink(self.invalid_topic, self.bootstrap_servers)

        # Create and process streams
        streams = [
            env.from_source(source, WatermarkStrategy.no_watermarks(), f"Source {i+1}")
            for i, source in enumerate(sources)
        ]

        combined_stream = streams[0]
        for stream in streams[1:]:
            combined_stream = combined_stream.union(stream)

        validated_stream = combined_stream.map(
            validate_schema, output_type=Types.STRING()
        )

        # Route to appropriate sinks
        validated_stream.filter(lambda x: json.loads(x)["valid"] == "INVALID").sink_to(
            invalid_sink
        )
        validated_stream.filter(lambda x: json.loads(x)["valid"] == "VALID").sink_to(
            valid_sink
        )
