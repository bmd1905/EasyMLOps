import argparse
import io
import json
import os
from datetime import datetime
from time import sleep

import avro
import avro.io
import avro.schema
import numpy as np
from dotenv import load_dotenv
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from schema_registry.client import SchemaRegistryClient, schema

load_dotenv()

OUTPUT_TOPICS = os.getenv("KAFKA_OUTPUT_TOPICS", "raw-events-topic")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
SCHEMA_REGISTRY_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "schema-registry:8081")

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a Kafka topic with driver stats events. Setup will teardown before beginning emitting events.",
)
parser.add_argument(
    "-b",
    "--bootstrap_servers",
    default=BOOTSTRAP_SERVERS,
    help="Where the bootstrap server is",
)
parser.add_argument(
    "-s",
    "--schema_registry_server",
    default=SCHEMA_REGISTRY_SERVER,
    help="Where to host schema",
)
parser.add_argument(
    "-c",
    "--avro_schemas_path",
    default=os.path.join(os.path.dirname(__file__), "avro_schemas"),
    help="Folder containing all generated avro schemas",
)

args = parser.parse_args()

# Define some constants
NUM_DEVICES = 1


def create_topic(admin, topic_name):
    # Create topic if not exists
    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin.create_topics([topic])
        print(f"A new topic {topic_name} has been created!")
    except Exception:
        print(f"Topic {topic_name} already exists. Skipping creation!")
        pass


def create_streams(servers, avro_schemas_path, schema_registry_client):
    producer = None
    admin = None

    # Add retry logic for Kafka connection
    for _ in range(10):
        try:
            producer = KafkaProducer(bootstrap_servers=servers)
            admin = KafkaAdminClient(bootstrap_servers=servers)
            print("SUCCESS: instantiated Kafka admin and producer")
            break
        except Exception as e:
            print(
                f"Trying to instantiate admin and producer with bootstrap servers {servers} with error {e}"
            )
            sleep(10)
            pass

    # Add retry logic for schema registry
    for _ in range(10):
        try:
            # Test schema registry connection
            schema_registry_client.get_subjects()
            print("SUCCESS: connected to schema registry")
            break
        except Exception as e:
            print(
                f"Failed to connect to schema registry: {e}. Retrying in 10 seconds..."
            )
            sleep(10)
    else:
        raise Exception("Failed to connect to schema registry after 10 attempts")

    while True:
        try:
            record = {}
            # Make event one more year recent to simulate fresher data
            record["created"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            record["device_id"] = np.random.randint(low=0, high=NUM_DEVICES)

            # Read columns from schema
            avro_schema_path = f"{avro_schemas_path}/schema_{record['device_id']}.avsc"
            with open(avro_schema_path, "r") as f:
                parsed_avro_schema = json.loads(f.read())

            for field in parsed_avro_schema["fields"]:
                if field["name"] not in ["created", "device_id"]:
                    record[field["name"]] = np.random.rand()

            # serialize the message data using the schema
            avro_schema = avro.schema.Parse(open(avro_schema_path, "r").read())
            writer = avro.io.DatumWriter(avro_schema)
            bytes_writer = io.BytesIO()
            # Write the Confluence "Magic Byte"
            bytes_writer.write(bytes([0]))

            # Get topic name for this device
            topic_name = OUTPUT_TOPICS

            # Check if schema exists in schema registry,
            # if not, register one
            schema_version_info = schema_registry_client.check_version(
                f"{topic_name}-schema", schema.AvroSchema(parsed_avro_schema)
            )
            if schema_version_info is not None:
                schema_id = schema_version_info.schema_id
                print(
                    "Found an existing schema ID: {}. Skipping creation!".format(
                        schema_id
                    )
                )
            else:
                schema_id = schema_registry_client.register(
                    f"{topic_name}-schema", schema.AvroSchema(parsed_avro_schema)
                )

            # Write schema ID
            bytes_writer.write(int.to_bytes(schema_id, 4, byteorder="big"))

            # Write data
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(record, encoder)

            # Create a new topic for this device id if not exists
            create_topic(admin, topic_name=topic_name)

            # Send messages to this topic
            producer.send(topic_name, value=bytes_writer.getvalue(), key=None)
            print(record)
            sleep(10)
        except Exception as e:
            print(f"Error creating stream: {e}. Retrying in 10 seconds...")
            sleep(10)


def teardown_stream(topic_name, servers=["localhost:9092"]):
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        print(admin.delete_topics([topic_name]))
        print(f"Topic {topic_name} deleted")
    except Exception as e:
        print(str(e))
        pass


if __name__ == "__main__":
    parsed_args = vars(args)
    mode = parsed_args["mode"]
    servers = parsed_args["bootstrap_servers"]
    schema_registry_server = parsed_args["schema_registry_server"]

    # Tear down all previous streams
    print("Tearing down all existing topics!")
    for device_id in range(NUM_DEVICES):
        try:
            teardown_stream(OUTPUT_TOPICS, [servers])
        except Exception as e:
            print(f"Topic device_{device_id} does not exist. Skipping...!")

    if mode == "setup":
        avro_schemas_path = parsed_args["avro_schemas_path"]
        schema_registry_client = SchemaRegistryClient(url=schema_registry_server)
        create_streams([servers], avro_schemas_path, schema_registry_client)
