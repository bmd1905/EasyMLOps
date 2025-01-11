import argparse
import json
import os
import random
import shutil

import numpy as np


def main(args):
    # Clean up the avro schema folder if exists,
    # then create a new folder
    if os.path.exists(args["schema_folder"]):
        shutil.rmtree(args["schema_folder"])
    os.mkdir(args["schema_folder"])

    # Loop over the number of schemas
    # to generate a schema
    for schema_idx in range(args["num_schemas"]):
        num_features = np.random.randint(args["min_features"], args["max_features"])
        sampled_features = random.sample(range(args["max_features"]), num_features)

        # Initialize schema template
        schema = {
            "doc": "Sample schema to help you get started.",
            "fields": [
                {"name": "device_id", "type": "int"},
                {"name": "created", "type": "string"},
            ],
            "name": "Device",
            "namespace": "example.avro",
            "type": "record",
        }

        # Add new features to the schema template
        for feature_idx in sampled_features:
            schema["fields"].append({"name": f"feature_{feature_idx}", "type": "float"})

        # Write this schema to the Avro output folder
        with open(f'{args["schema_folder"]}/schema_{schema_idx}.avsc', "w+") as f:
            json.dump(schema, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        "--num_schemas",
        default=1,
        type=int,
        help="Number of avro schemas to generate.",
    )
    parser.add_argument(
        "-m",
        "--min_features",
        default=2,
        type=int,
        help="Minumum number of features for each device",
    )
    parser.add_argument(
        "-a",
        "--max_features",
        default=10,
        type=int,
        help="Maximum number of features for each device",
    )
    parser.add_argument(
        "-o",
        "--schema_folder",
        default="./avro_schemas",
        help="Folder containing all generated avro schemas",
    )
    args = vars(parser.parse_args())
    main(args)
