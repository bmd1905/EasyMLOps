from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema,
    KafkaSink,
)


def build_sink(
    topic: str = "output-topic",
    bootstrap_servers: str = "kafka:9092",
) -> KafkaSink:
    """
    Build a Kafka sink.

    :param topic: The topic to write to.
    :param bootstrap_servers: The bootstrap servers.
    :return: The Kafka sink.
    """
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
