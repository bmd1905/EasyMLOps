# entities.py
from feast import Entity, ValueType

# User entity
user = Entity(
    name="user_id",
    value_type=ValueType.INT64,
    description="User identifier",
    join_keys=["user_id"],
)

# Product entity
product = Entity(
    name="product_id",
    value_type=ValueType.INT64,
    description="Product identifier",
    join_keys=["product_id"],
)
