from typing import Any, Dict

import pandas as pd
from common.scripts.db_utils import batch_insert_data, create_schema_and_table
from common.scripts.schemas.event_schema import EventSchema

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@task()
def check_dwh_connection() -> bool:
    """Check Data Warehouse connection"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")
        postgres_hook.get_conn()
        return True
    except Exception as e:
        raise Exception(f"Failed to connect to Data Warehouse: {str(e)}")


@task()
def save_to_dwh(processed_data: Dict[str, Any]) -> None:
    """Save processed data to Data Warehouse"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")
        df = pd.DataFrame(processed_data["data"])

        # Get column definitions from schema
        columns = [col.name for col in EventSchema.get_columns()]
        df = df[columns]

        # Apply data type conversions
        df = df.astype(
            {
                "product_id": "int",
                "category_id": "int",
                "user_id": "int",
                "price": "float",
            }
        )

        create_schema_and_table(postgres_hook)
        batch_insert_data(postgres_hook, df)

    except Exception as e:
        raise Exception(f"Failed to save data to Data Warehouse: {str(e)}")
