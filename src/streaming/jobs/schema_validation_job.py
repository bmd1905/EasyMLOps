import json
import os
import sys
import time
from datetime import datetime
from threading import Lock
from typing import Any, Dict, Tuple

from loguru import logger
from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

from ..connectors.sinks.kafka_sink import build_sink
from ..connectors.sources.kafka_source import build_source
from ..jobs.base import FlinkJob

# Configure logger
logger.remove()
logger.add(
    sys.stderr,
    level=os.getenv("LOG_LEVEL", "INFO"),
)


class RequestCounter:
    def __init__(self):
        self.valid_count = 0
        self.invalid_count = 0
        self.last_log_time = time.time()
        self.lock = Lock()
        self.processing_times = []
        self.samples_threshold = 1_000

    def increment_valid(self):
        with self.lock:
            self.valid_count += 1
            self._check_and_log()

    def increment_invalid(self):
        with self.lock:
            self.invalid_count += 1
            self._check_and_log()

    def add_processing_time(self, processing_time: float):
        with self.lock:
            self.processing_times.append(processing_time)
            if len(self.processing_times) >= self.samples_threshold:
                avg_time = sum(self.processing_times) / len(self.processing_times)
                logger.info(
                    f"Average processing time over last {self.samples_threshold} records: {avg_time:.2f}ms"
                )
                self.processing_times = []  # Reset for next batch

    def _check_and_log(self):
        current_time = time.time()
        if current_time - self.last_log_time >= 1.0:
            total = self.valid_count + self.invalid_count
            logger.info(
                f"Processed {total} records in the last second (Valid: {self.valid_count}, Invalid: {self.invalid_count})"
            )
            self.valid_count = 0
            self.invalid_count = 0
            self.last_log_time = current_time


request_counter = RequestCounter()


def validate_field_type(value: Any, field_def: Dict) -> bool:
    """Validate a field's type against its schema definition."""
    # Handle null values first
    if value is None:
        # If type is a list and includes "null", or if field is optional, null is valid
        if isinstance(field_def.get("type"), list):
            return "null" in field_def["type"]
        return field_def.get("optional", False)

    # Get the field type
    field_type = (
        field_def.get("name")
        if field_def.get("name", "").startswith("io.debezium.time")
        else field_def.get("type")
    )

    # Handle array type definitions (like ["null", "string"])
    if isinstance(field_type, list):
        # Get the non-null type for validation
        field_type = next((t for t in field_type if t != "null"), field_type[0])

    type_validators = {
        "string": lambda v: isinstance(v, str),
        "int64": lambda v: isinstance(v, int),
        "long": lambda v: isinstance(v, int),
        "double": lambda v: isinstance(v, (float, int)),
        "boolean": lambda v: isinstance(v, bool),
        "io.debezium.time.MicroTimestamp": lambda v: isinstance(v, int) and v >= 0,
        "io.debezium.time.Timestamp": lambda v: isinstance(v, int) and v >= 0,
        "io.debezium.time.ZonedTimestamp": lambda v: isinstance(v, str)
        and _is_valid_timestamp(v),
    }

    validator = type_validators.get(field_type)
    if not validator:
        logger.warning(f"No validator found for type: {field_type}")
        return True  # Skip validation for unknown types

    is_valid = validator(value)
    logger.debug(
        f"Validating type for value: {value}, type: {field_type}, result: {is_valid}"
    )
    return is_valid


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
    logger.debug(f"Timestamp {value} is not valid")
    return False


def get_field_name(field_def: Dict) -> str:
    """Get field name from field definition, handling both schema formats."""
    return field_def.get("field") or field_def.get("name")


def validate_field_value(value: Any, field_def: Dict) -> Tuple[bool, str]:
    """Validate a field's value constraints."""
    field_name = get_field_name(field_def)
    logger.debug(f"Validating value for field: {field_name}, value: {value}")

    if field_name == "price" and value is not None:
        is_valid = value > 0
        error_message = "Price should be greater than 0" if not is_valid else None
        logger.debug(f"Price validation result: {is_valid}, error: {error_message}")
        return (is_valid, error_message)

    if field_name == "event_type" and value is not None:
        valid_types = ["view", "cart", "purchase", "remove_from_cart"]
        is_valid = value in valid_types
        error_message = (
            f"Event type should be one of: {', '.join(valid_types)}"
            if not is_valid
            else None
        )
        logger.debug(
            f"Event type validation result: {is_valid}, error: {error_message}"
        )
        return (
            is_valid,
            error_message,
        )

    logger.debug(f"No specific validation for field: {field_name}")
    return True, None


def validate_schema(record: str) -> str:
    """Validate a record against its schema."""
    start_time = time.time()
    logger.debug(f"Validating record: {record}")

    try:
        record_dict = json.loads(record) if isinstance(record, str) else record
        logger.debug(f"Record after json load: {record_dict}")

        if "metadata" not in record_dict:
            record_dict["metadata"] = {}
            logger.debug(f"Added metadata to record: {record_dict}")

        schema = record_dict["schema"]
        payload = record_dict["payload"]
        logger.debug(f"Schema: {schema}, Payload: {payload}")

        # Validate each field
        for field in schema["fields"]:
            field_name = get_field_name(field)

            # Check if required field is present
            if field_name not in payload and not field.get("optional", False):
                record_dict.update(
                    {
                        "valid": "INVALID",
                        "error_message": f"Field {field_name} is required",
                        "error_type": "SCHEMA_VALIDATION_ERROR",
                    }
                )
                logger.debug(
                    f"Field {field_name} is required but not present. Record marked as invalid: {record_dict}"
                )
                request_counter.increment_invalid()
                processing_time = (time.time() - start_time) * 1000
                request_counter.add_processing_time(processing_time)
                return json.dumps(record_dict)

            # Skip validation for missing optional fields
            if field_name not in payload:
                logger.debug(
                    f"Field {field_name} is optional and not present. Skipping validation."
                )
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
                logger.debug(
                    f"Field {field_name} has invalid type. Record marked as invalid: {record_dict}"
                )
                request_counter.increment_invalid()
                processing_time = (time.time() - start_time) * 1000
                request_counter.add_processing_time(processing_time)
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
                logger.debug(
                    f"Field {field_name} has invalid value. Record marked as invalid: {record_dict}"
                )
                request_counter.increment_invalid()
                processing_time = (time.time() - start_time) * 1000
                request_counter.add_processing_time(processing_time)
                return json.dumps(record_dict)

        # All validations passed
        record_dict.update(
            {"valid": "VALID", "error_message": None, "error_type": None}
        )
        logger.debug(f"All validations passed. Record marked as valid: {record_dict}")

        record_dict["metadata"]["processed_at"] = datetime.utcnow().isoformat()
        logger.debug(f"Record after processing: {record_dict}")
        request_counter.increment_valid()
        processing_time = (time.time() - start_time) * 1000
        request_counter.add_processing_time(processing_time)
        return json.dumps(record_dict)

    except Exception as e:
        data = {
            "valid": "INVALID",
            "error_message": f"Processing error: {str(e)}",
            "error_type": "PROCESSING_ERROR",
            "raw_data": record,
            "metadata": {"processed_at": datetime.utcnow().isoformat()},
        }
        logger.error(f"Error processing record: {record}, error: {e}, data: {data}")
        request_counter.increment_invalid()
        processing_time = (time.time() - start_time) * 1000
        request_counter.add_processing_time(processing_time)
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
