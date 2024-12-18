from typing import Dict, List


class FactEventSchema:
    """Schema for fact events table"""

    table_schema: Dict[str, str] = {
        "event_id": "BIGSERIAL PRIMARY KEY",
        "event_type": "VARCHAR(50)",
        "user_id": "BIGINT REFERENCES dim_user(user_id)",
        "product_id": "BIGINT REFERENCES dim_product(product_id)",
        "category_id": "BIGINT REFERENCES dim_category(category_id)",
        "event_date": "DATE REFERENCES dim_date(event_date)",
        "event_timestamp": "TIMESTAMP",
        "user_session": "VARCHAR(255)",
        "events_in_session": "INTEGER",
    }
    indexes: List[str] = [
        "CREATE INDEX idx_fact_events_user_id ON fact_events (user_id)",
        "CREATE INDEX idx_fact_events_product_id ON fact_events (product_id)",
        "CREATE INDEX idx_fact_events_category_id ON fact_events (category_id)",
        "CREATE INDEX idx_fact_events_event_date ON fact_events (event_date)",
        "CREATE INDEX idx_fact_events_event_type ON fact_events (event_type)",
        "CREATE INDEX idx_fact_events_user_session ON fact_events (user_session)",
    ]
