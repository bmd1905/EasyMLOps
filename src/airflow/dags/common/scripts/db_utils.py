import logging
from typing import Any, Dict, List

import pandas as pd
from psycopg2.extras import execute_values

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def create_schema_and_table(postgres_hook: PostgresHook, schema_class: Any, table_name: str) -> None:
    """Create table and indexes based on schema definition"""
    try:
        # Create table
        columns = [f"{col} {dtype}" for col, dtype in schema_class.table_schema.items()]
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        );
        """
        postgres_hook.run(create_table_sql)

        # Create indexes with IF NOT EXISTS
        for index_sql in schema_class.indexes:
            # Add IF NOT EXISTS to index creation
            if "CREATE INDEX" in index_sql:
                index_sql = index_sql.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS")
            postgres_hook.run(index_sql)
            
    except Exception as e:
        raise AirflowException(f"Failed to create schema and table: {str(e)}")


def batch_insert_data(postgres_hook: PostgresHook, df: pd.DataFrame, table_name: str) -> None:
    """Insert data in batches"""
    # Convert DataFrame to native Python types
    df = df.astype(object)  # Convert all columns to object type first
    df = df.where(pd.notnull(df), None)  # Replace NaN with None
    
    # Convert DataFrame to list of tuples with native Python types
    data = [tuple(x) for x in df.to_numpy()]
    
    # Get column names
    columns = df.columns.tolist()
    
    # Create placeholders for SQL query
    placeholders = ','.join(['%s'] * len(columns))
    
    # Construct insert query
    insert_query = f"""
    INSERT INTO {table_name} ({','.join(columns)})
    VALUES ({placeholders})
    ON CONFLICT DO NOTHING;
    """
    
    # Execute in batches
    batch_size = 1000
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        postgres_hook.run(insert_query, parameters=batch)
