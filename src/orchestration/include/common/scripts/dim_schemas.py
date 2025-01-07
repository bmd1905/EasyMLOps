from typing import Dict, List


class DimUserSchema:
    """Schema for user dimension table"""

    table_schema: Dict[str, str] = {
        "user_id": "BIGINT PRIMARY KEY",
    }
    indexes: List[str] = [
        "CREATE INDEX idx_dim_user_id ON dim_user (user_id)",
    ]


class DimProductSchema:
    """Schema for product dimension table"""

    table_schema: Dict[str, str] = {
        "product_id": "BIGINT PRIMARY KEY",
        "brand": "VARCHAR(255)",
        "price": "DECIMAL(10,2)",
        "price_tier": "VARCHAR(50)",
    }
    indexes: List[str] = [
        "CREATE INDEX idx_dim_product_id ON dim_product (product_id)",
        "CREATE INDEX idx_dim_product_brand ON dim_product (brand)",
    ]


class DimCategorySchema:
    """Schema for category dimension table"""

    table_schema: Dict[str, str] = {
        "category_id": "BIGINT PRIMARY KEY",
        "category_code": "VARCHAR(255)",
        "category_l1": "VARCHAR(255)",
        "category_l2": "VARCHAR(255)",
        "category_l3": "VARCHAR(255)",
    }
    indexes: List[str] = [
        "CREATE INDEX idx_dim_category_id ON dim_category (category_id)",
        "CREATE INDEX idx_dim_category_l1 ON dim_category (category_l1)",
    ]


class DimDateSchema:
    """Schema for date dimension table"""

    table_schema: Dict[str, str] = {
        "event_date": "DATE PRIMARY KEY",
        "event_hour": "INTEGER",
        "day_of_week": "VARCHAR(50)",
    }
    indexes: List[str] = [
        "CREATE INDEX idx_dim_date_date ON dim_date (event_date)",
        "CREATE INDEX idx_dim_date_hour ON dim_date (event_hour)",
    ]
