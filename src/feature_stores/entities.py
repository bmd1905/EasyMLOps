from feast import Entity

device = Entity(
    name="device",
    join_keys=["device_id"],
    description="device id",
)
