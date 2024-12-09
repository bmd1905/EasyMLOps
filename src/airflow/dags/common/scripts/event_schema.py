from dataclasses import dataclass
from typing import List


@dataclass
class PostgresColumn:
    name: str
    type: str
    nullable: bool


class EventSchema:
    @staticmethod
    def get_columns() -> List[PostgresColumn]:
        """Define the PostgreSQL table columns"""
        return [
            PostgresColumn("event_time", "TEXT", False),
            PostgresColumn("event_type", "TEXT", False),
            PostgresColumn("product_id", "BIGINT", False),
            PostgresColumn("category_id", "BIGINT", False),
            PostgresColumn("category_code", "TEXT", True),
            PostgresColumn("brand", "TEXT", True),
            PostgresColumn("price", "DOUBLE PRECISION", False),
            PostgresColumn("user_id", "BIGINT", False),
            PostgresColumn("user_session", "TEXT", False),
            PostgresColumn("processed_date", "TIMESTAMP WITH TIME ZONE", True),
            PostgresColumn("processing_pipeline", "TEXT", True),
            PostgresColumn("valid", "TEXT", True),
            PostgresColumn("record_hash", "VARCHAR(64)", False),
        ]
