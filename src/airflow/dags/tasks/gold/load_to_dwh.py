from typing import Any, Dict

import pandas as pd
from common.scripts.db_utils import batch_insert_data, create_schema_and_table
from common.scripts.dim_schemas import (
    DimCategorySchema,
    DimDateSchema,
    DimProductSchema,
    DimUserSchema,
)
from common.scripts.event_schema import EventSchema
from common.scripts.fact_schemas import FactEventSchema

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_dim_user(df: pd.DataFrame) -> pd.DataFrame:
    """Create user dimension table"""
    dim_user = df[["user_id"]].drop_duplicates()
    return dim_user


def create_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    """Create product dimension table"""
    dim_product = df[["product_id", "brand", "price", "price_tier"]].drop_duplicates()
    return dim_product


def create_dim_category(df: pd.DataFrame) -> pd.DataFrame:
    """Create category dimension table"""
    dim_category = df[
        ["category_id", "category_code", "category_l1", "category_l2", "category_l3"]
    ].drop_duplicates()
    return dim_category


def create_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """Create date dimension table"""
    dim_date = df[["event_date", "event_hour", "day_of_week"]].drop_duplicates()
    return dim_date


def create_fact_events(df: pd.DataFrame, dims: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Create fact table with foreign keys to dimensions"""
    fact_events = df[
        [
            "event_type",
            "user_id",
            "product_id",
            "category_id",
            "event_date",
            "event_timestamp",
            "user_session",
            "events_in_session",
        ]
    ]
    return fact_events


@task()
def load_dimensions_and_facts(transformed_data: Dict[str, Any]) -> bool:
    """Load dimensional model into Data Warehouse"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")
        df = pd.DataFrame(transformed_data["data"])

        # Create dimensions
        dims = {
            "dim_user": create_dim_user(df),
            "dim_product": create_dim_product(df),
            "dim_category": create_dim_category(df),
            "dim_date": create_dim_date(df),
        }

        # Create fact table
        fact_events = create_fact_events(df, dims)

        # Create schemas and tables
        for table_name, dim_df in dims.items():
            schema_class = globals()[f"{table_name.title()}Schema"]
            create_schema_and_table(postgres_hook, schema_class, table_name)
            batch_insert_data(postgres_hook, dim_df, table_name)

        # Create and load fact table
        create_schema_and_table(postgres_hook, FactEventSchema, "fact_events")
        batch_insert_data(postgres_hook, fact_events, "fact_events")

        # Create useful views
        create_analytical_views(postgres_hook)

        return True

    except Exception as e:
        raise Exception(f"Failed to load dimensional model: {str(e)}")


def create_analytical_views(postgres_hook: PostgresHook) -> None:
    """Create useful views for analysis"""
    views = {
        "vw_user_session_summary": """
            CREATE OR REPLACE VIEW vw_user_session_summary AS
            SELECT 
                u.user_id,
                f.user_session,
                COUNT(*) as event_count,
                MIN(d.event_timestamp) as session_start,
                MAX(d.event_timestamp) as session_end,
                COUNT(DISTINCT p.product_id) as unique_products_viewed
            FROM fact_events f
            JOIN dim_user u ON f.user_id = u.user_id
            JOIN dim_date d ON f.event_date = d.event_date
            JOIN dim_product p ON f.product_id = p.product_id
            GROUP BY u.user_id, f.user_session
        """,
        "vw_category_performance": """
            CREATE OR REPLACE VIEW vw_category_performance AS
            SELECT 
                c.category_l1,
                c.category_l2,
                c.category_l3,
                COUNT(*) as view_count,
                COUNT(DISTINCT u.user_id) as unique_users,
                AVG(p.price) as avg_price
            FROM fact_events f
            JOIN dim_category c ON f.category_id = c.category_id
            JOIN dim_user u ON f.user_id = u.user_id
            JOIN dim_product p ON f.product_id = p.product_id
            GROUP BY c.category_l1, c.category_l2, c.category_l3
        """,
    }

    for view_name, view_sql in views.items():
        postgres_hook.run(view_sql)
