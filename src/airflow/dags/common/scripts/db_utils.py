import logging

import pandas as pd
from psycopg2.extras import execute_values

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def create_schema_and_table(postgres_hook: PostgresHook) -> None:
    """Create the schema and table if they don't exist"""
    try:
        postgres_hook.run("CREATE SCHEMA IF NOT EXISTS dwh;")
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS dwh.processed_events (
                event_time TEXT NOT NULL,
                event_type TEXT NOT NULL,
                product_id BIGINT NOT NULL,
                category_id BIGINT NOT NULL,
                category_code TEXT NULL,
                brand TEXT NULL,
                price DOUBLE PRECISION NOT NULL,
                user_id BIGINT NOT NULL,
                user_session TEXT NOT NULL,
                processed_date TIMESTAMP WITH TIME ZONE NULL,
                processing_pipeline TEXT NULL,
                valid TEXT NULL,
                record_hash VARCHAR(64) NOT NULL
            );
        """
        postgres_hook.run(create_table_sql)

        create_index_sql = """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_events_record_hash 
            ON dwh.processed_events(record_hash);
        """
        postgres_hook.run(create_index_sql)
    except Exception as e:
        raise AirflowException(f"Failed to create schema and table: {str(e)}")


def batch_insert_data(postgres_hook: PostgresHook, df: pd.DataFrame) -> None:
    """Insert data in batches with deduplication"""
    try:
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        values = df.to_records(index=False).tolist()

        insert_sql = """
            INSERT INTO dwh.processed_events (
                event_time, event_type, product_id, category_id,
                category_code, brand, price, user_id, user_session,
                processed_date, processing_pipeline, valid, record_hash
            ) VALUES %s 
            ON CONFLICT (record_hash) DO NOTHING
        """

        execute_values(cur, insert_sql, values)
        conn.commit()
        cur.close()
    except Exception as e:
        raise AirflowException(f"Failed to insert data: {str(e)}")
