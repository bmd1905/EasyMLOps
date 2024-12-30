import logging
from typing import Any

import pandas as pd
from psycopg2.extras import execute_values

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def create_schema_and_table(
    postgres_hook: PostgresHook, schema_class: Any, table_name: str
) -> None:
    """Create table and indexes based on schema definition"""
    try:
        # Extract schema name and table name
        schema_name = table_name.split(".")[0]
        table_only = table_name.split(".")[1]  # noqa: F841

        # Create schema
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
        postgres_hook.run(create_schema_sql)

        # Create table
        columns = [f"{col} {dtype}" for col, dtype in schema_class.table_schema.items()]
        create_table_sql = f"""
        DROP TABLE IF EXISTS {table_name} CASCADE;
        CREATE TABLE {table_name} (
            {', '.join(columns)}
        );
        """
        postgres_hook.run(create_table_sql)

        # Create indexes with IF NOT EXISTS
        for index_sql in schema_class.indexes:
            # Add IF NOT EXISTS to index creation
            if "CREATE INDEX" in index_sql:
                index_sql = index_sql.replace(
                    "CREATE INDEX", "CREATE INDEX IF NOT EXISTS"
                )
            postgres_hook.run(index_sql)

    except Exception as e:
        raise AirflowException(f"Failed to create schema and table: {str(e)}")


def batch_insert_data(
    postgres_hook: PostgresHook, df: pd.DataFrame, table_name: str
) -> None:
    """Insert data in batches"""
    try:
        # Convert DataFrame to native Python types
        df = df.astype(object)  # Convert all columns to object type first
        df = df.where(pd.notnull(df), None)  # Replace NaN with None

        # Get connection and cursor
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # Get column names
        columns = df.columns.tolist()

        # Convert DataFrame to list of tuples
        values = df.to_records(index=False).tolist()

        # Construct insert query
        insert_query = f"""
            INSERT INTO {table_name} ({','.join(columns)})
            VALUES %s
            ON CONFLICT DO NOTHING;
        """

        # Execute in batches using execute_values
        batch_size = 1000
        execute_values(cur, insert_query, values, page_size=batch_size)

        # Commit the transaction
        conn.commit()

    except Exception as e:
        raise AirflowException(f"Failed to insert data: {str(e)}")
    finally:
        if "cur" in locals():
            cur.close()
