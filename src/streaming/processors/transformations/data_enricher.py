import json


def merge_features(record):
    """
    Merged feature columns into one single data column
    and keep other columns unchanged.
    """
    # Convert Row to dict
    record = json.loads(record)

    # Create a dictionary of all features
    # and create a data column for this
    data = {}
    for key in record["payload"]:
        if key.startswith("feature"):
            data[key] = record["payload"][key]

    # Convert the data column to string
    # and add other features back to record
    return json.dumps(
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {"field": "device_id", "type": "int8", "optional": "false"},
                    {"field": "created", "type": "string", "optional": "false"},
                    {"field": "data", "type": "string", "optional": "false"},
                ],
            },
            "payload": {
                "data": json.dumps(data),
            },
        }
    )


def filter_small_features(record):
    """
    Skip records containing a feature that is smaller than 0.5.
    """
    # Convert Row to dict
    record = json.loads(record)

    # for key in record["payload"]:
    #     if key.startswith("feature"):
    #         if record["payload"][key] < 0.5:
    #             return False

    print("Found record: ", record)
    return True
