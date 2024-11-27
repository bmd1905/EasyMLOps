import os

from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

from .connectors.sinks.kafka_sink import build_sink
from .connectors.sources.kafka_source import build_source
from .processors.transformations.data_enricher import (
    filter_small_features,
    merge_features,
)

JARS_PATH = f"{os.getcwd()}/src/streaming/connectors/config/jars/"


TOPICS = "input-topic"
GROUP_ID = "flink-group"
OUTPUT_TOPIC = "output-topic"
BOOTSTRAP_SERVERS = "localhost:9092"


def create_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()

    # The other commented lines are for Avro format
    env.add_jars(
        f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar",
        f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
    )

    # Create source
    source = build_source(
        topics=TOPICS,
        group_id=GROUP_ID,
        bootstrap_servers=BOOTSTRAP_SERVERS,
    )

    # Create sink
    sink = build_sink(
        topic=OUTPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
    )

    # Build pipeline
    env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").filter(
        filter_small_features
    ).map(merge_features, output_type=Types.STRING()).sink_to(sink=sink)

    return env


if __name__ == "__main__":
    env = create_pipeline()
    env.execute("Streaming Pipeline")
    print("Your job has been started successfully!")
