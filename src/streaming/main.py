import argparse
import os
from abc import ABC, abstractmethod
from typing import Dict, Type

from pyflink.datastream import StreamExecutionEnvironment


class FlinkJob(ABC):
    """Base class for all Flink jobs"""

    @abstractmethod
    def create_pipeline(self, env: StreamExecutionEnvironment):
        """Create and return the job pipeline"""
        pass

    @property
    @abstractmethod
    def job_name(self) -> str:
        """Return the name of the job"""
        pass


class FeatureEnrichmentJob(FlinkJob):
    """Your current feature enrichment job"""

    def __init__(self):
        self.jars_path = f"{os.getcwd()}/src/streaming/connectors/config/jars/"
        self.input_topics = os.getenv("KAFKA_INPUT_TOPICS", "raw-events-topic")
        self.group_id = os.getenv("KAFKA_GROUP_ID", "flink-group")
        self.output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "validated-events-topic")
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    @property
    def job_name(self) -> str:
        return "feature_enrichment"

    def create_pipeline(self, env: StreamExecutionEnvironment):
        from pyflink.common import WatermarkStrategy
        from pyflink.common.typeinfo import Types

        from .connectors.sinks.kafka_sink import build_sink
        from .connectors.sources.kafka_source import build_source
        from .processors.transformations.data_enricher import (
            filter_small_features,
            merge_features,
        )

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

        # Create sink
        sink = build_sink(
            topic=self.output_topic,
            bootstrap_servers=self.bootstrap_servers,
        )

        # Build pipeline
        env.from_source(
            source, WatermarkStrategy.no_watermarks(), "Kafka Source"
        ).filter(filter_small_features).map(
            merge_features, output_type=Types.STRING()
        ).sink_to(sink=sink)


def get_available_jobs() -> Dict[str, Type[FlinkJob]]:
    """Return a dictionary of available jobs"""
    return {
        "feature_enrichment": FeatureEnrichmentJob,
        # Add more jobs here as they are created
    }


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run a Flink job")
    parser.add_argument(
        "job_name", choices=get_available_jobs().keys(), help="Name of the job to run"
    )
    args = parser.parse_args()

    # Get the job class and create an instance
    job_class = get_available_jobs()[args.job_name]
    job = job_class()

    # Create and execute the pipeline
    env = StreamExecutionEnvironment.get_execution_environment()
    job.create_pipeline(env)
    env.execute(f"{job.job_name} Pipeline")
    print(f"Job {job.job_name} has been started successfully!")


if __name__ == "__main__":
    main()
