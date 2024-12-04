import argparse
import json
import os
from datetime import datetime
from time import sleep

import numpy as np
from bson import json_util
from dotenv import load_dotenv
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

load_dotenv()

GROUP_ID = os.getenv("KAFKA_GROUP_ID", "flink-group")
OUTPUT_TOPICS = os.getenv("KAFKA_OUTPUT_TOPICS", "raw-events-topic")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")

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
    "-c",
    "--schemas_path",
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


def create_streams(servers, schemas_path):
    producer = None
    admin = None
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

    while True:
        data = {}
        # Make event one more year recent to simulate fresher data
        data["created"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data["device_id"] = np.random.randint(low=0, high=NUM_DEVICES)

        # Read columns from schema
        schema_path = f"{schemas_path}/schema_{data['device_id']}.avsc"
        with open(schema_path, "r") as f:
            parsed_schema = json.loads(f.read())

        for field in parsed_schema["fields"]:
            if field["name"] not in ["created", "device_id"]:
                data[field["name"]] = np.random.rand()

        # Get topic name for this device
        topic_name = OUTPUT_TOPICS

        # Create a new topic for this device id if not exists
        create_topic(admin, topic_name=topic_name)

        # Create the record including schema, and data,
        # ref: https://stackoverflow.com/a/76511956
        record = {
            "schema": {"type": "struct", "fields": parsed_schema["fields"]},
            "payload": data,
        }

        # Send messages to this topic
        producer.send(
            topic_name, json.dumps(record, default=json_util.default).encode("utf-8")
        )
        print(record)
        sleep(2)


def teardown_stream(topic_name, servers=[BOOTSTRAP_SERVERS]):
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

    # Tear down all previous streams
    print("Tearing down all existing topics!")
    for device_id in range(NUM_DEVICES):
        try:
            teardown_stream(f"{OUTPUT_TOPICS}_{device_id}", [servers])
        except Exception as e:
            print(f"Topic {OUTPUT_TOPICS}_{device_id} does not exist. Skipping...!")

    if mode == "setup":
        schemas_path = parsed_args["schemas_path"]
        create_streams([servers], schemas_path)
