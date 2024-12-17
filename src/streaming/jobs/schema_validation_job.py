import json
import os
from datetime import datetime
from typing import Any, Dict, List, Union
from threading import Lock
import time

from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

from ..connectors.sinks.kafka_sink import build_sink
from ..connectors.sources.kafka_source import build_source
from ..jobs.base import FlinkJob


# Replace the global counter with a more sophisticated counter class
class RequestCounter:
    def __init__(self):
        self.count = 0
        self.last_log_time = time.time()
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.count += 1
            current_time = time.time()
            # Log every second
            if current_time - self.last_log_time >= 1.0:
                print(f"Processed {self.count} records in the last second")
                self.count = 0
                self.last_log_time = current_time


# Initialize the counter
request_counter = RequestCounter()


def validate_field_type(
    field_value: Any, field_type: Union[str, List[str]], field_name: str
) -> bool:
    """
    Validate a single field's type against the expected schema type.
    """
    # Handle union types (e.g., ['null', 'string'])
    if isinstance(field_type, list):
        # If any type validation passes, the field is valid
        return any(validate_field_type(field_value, t, field_name) for t in field_type)

    # Handle null values
    if field_type == "null":
        return field_value is None

    # Type mapping between schema types and Python types
    type_mapping = {
        "string": str,
        "long": (int,),  # Using tuple for multiple valid types
        "double": (float, int),  # Both float and int are valid for double
        "boolean": bool,
        "int": int,
    }

    if field_type not in type_mapping:
        raise ValueError(f"Unsupported type in schema: {field_type}")

    expected_types = type_mapping[field_type]
    if not isinstance(expected_types, tuple):
        expected_types = (expected_types,)

    # Special handling for string fields that should be datetime
    if field_type == "string" and field_name == "event_time":
        try:
            datetime.strptime(field_value, "%Y-%m-%d %H:%M:%S UTC")
            return True
        except ValueError:
            return False

    return isinstance(field_value, expected_types)


def validate_field_value(
    field_value: Any, field_type: Union[str, List[str]], field_name: str
) -> bool:
    """
    Validate a single field's value against the expected schema value.
    """
    # price should be greater than 0
    if field_name == "price":
        return field_value > 0, "Price should be greater than 0"

    # event type should be one of the following: "view", "cart", "purchase", "remove_from_cart"
    if field_name == "event_type":
        return (
            field_value in ["view", "cart", "purchase", "remove_from_cart"],
            "Event type should be one of the following: view, cart, purchase, remove_from_cart",
        )

    return True, None


def validate_schema_against_payload(schema: Dict, payload: Dict) -> bool:
    """
    Validate the schema against the payload.

    Args:
        schema: The schema definition containing field types
        payload: The data payload to validate

    Returns:
        bool: True if payload matches schema, False otherwise
    """
    if not isinstance(schema, dict) or not isinstance(payload, dict):
        return (
            False,
            "Schema and payload must be dictionaries",
            "SCHEMA_VALIDATION_ERROR",
        )

    if "type" not in schema or schema["type"] != "struct":
        return (
            False,
            "Schema must be a dictionary with a 'type' key set to 'struct'",
            "SCHEMA_VALIDATION_ERROR",
        )

    if "fields" not in schema:
        return (
            False,
            "Schema must contain a 'fields' key",
            "SCHEMA_VALIDATION_ERROR",
        )

    # Check all required fields are present and have correct types
    for field in schema["fields"]:
        field_name = field["name"]
        field_type = field["type"]

        # Check if field exists in payload
        if field_name not in payload:
            # If field has a default value, it's optional
            if "default" in field:
                continue
            return (
                False,
                f"Field {field_name} is required",
                "SCHEMA_VALIDATION_ERROR",
            )

        # Validate the field's type
        if not validate_field_type(payload[field_name], field_type, field_name):
            return (
                False,
                f"Field {field_name} has invalid type",
                "SCHEMA_VALIDATION_ERROR",
            )

        # Validate the field's value
        valid, error_message = validate_field_value(
            payload[field_name], field_type, field_name
        )
        if not valid:
            return False, error_message, "SCHEMA_VALIDATION_ERROR"

    return True, None, None


def validate_schema(record: str) -> dict:
    """
    Validate the schema of the record.

    Args:
        record: JSON string containing schema and payload

    Returns:
        dict: A dictionary containing validation results
    """
    # Increment request counter
    request_counter.increment()

    # Convert string to dict if needed
    record_dict = json.loads(record)  # if isinstance(record, str) else record

    payload = record_dict["payload"]
    schema = record_dict["schema"]

    # Validate schema against payload
    valid, error_message, error_type = validate_schema_against_payload(schema, payload)
    record_dict["valid"] = "VALID" if valid else "INVALID"
    record_dict["error_message"] = error_message
    record_dict["error_type"] = error_type
    # print("Schema validation result: ", valid)

    return json.dumps(record_dict)


class SchemaValidationJob(FlinkJob):
    def __init__(self):
        self.jars_path = f"{os.getcwd()}/src/streaming/connectors/config/jars/"
        self.input_topics = os.getenv("KAFKA_INPUT_TOPICS", "raw-events-topic")
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
        # Set parallelism
        env.set_parallelism(4)

        # Add required JARs
        env.add_jars(
            f"file://{self.jars_path}/flink-connector-kafka-1.17.1.jar",
            f"file://{self.jars_path}/kafka-clients-3.4.0.jar",
        )

        # Create source
        source = build_source(
            topics=self.input_topics,
            group_id=self.group_id,
            bootstrap_servers=self.bootstrap_servers,
        )

        # Create sinks
        valid_sink = build_sink(
            topic=self.valid_topic,
            bootstrap_servers=self.bootstrap_servers,
        )

        invalid_sink = build_sink(
            topic=self.invalid_topic,
            bootstrap_servers=self.bootstrap_servers,
        )

        # Build pipeline
        stream = env.from_source(
            source, WatermarkStrategy.no_watermarks(), "Schema Validation Job"
        )

        # Process stream and get valid and invalid streams
        validated_stream = stream.map(validate_schema, output_type=Types.STRING())

        # # Sink streams to respective topics
        validated_stream.filter(lambda x: "INVALID" in x).sink_to(sink=invalid_sink)
        validated_stream.filter(lambda x: "VALID" in x).sink_to(sink=valid_sink)
