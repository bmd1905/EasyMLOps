from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)


def build_source(
    topics: str = "input-topic",
    group_id: str = "flink-group",
    bootstrap_servers: str = "kafka:9092",
) -> KafkaSource:
    """
    Build a Kafka source.

    :param bootstrap_servers: The bootstrap servers.
    :param topics: The topics to read from.
    :param group_id: The group ID.
    :return: The Kafka source.
    """
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(topics)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
