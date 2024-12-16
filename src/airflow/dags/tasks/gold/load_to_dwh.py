from typing import Any, Dict

import pandas as pd
from common.scripts.db_utils import batch_insert_data, create_schema_and_table
from common.scripts.sql_utils import load_sql_template
from common.scripts.dim_schemas import (
    DimCategorySchema,
    DimDateSchema,
    DimProductSchema,
    DimUserSchema,
)
from common.scripts.fact_schemas import FactEventSchema

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from loguru import logger

logger = logger.bind(name=__name__)


def create_dim_user(df: pd.DataFrame) -> pd.DataFrame:
    """Create user dimension table"""
    logger.info("Creating user dimension table")
    dim_user = df[["user_id"]].copy()
    dim_user.loc[:, "user_id"] = dim_user["user_id"].astype(int)
    return dim_user.drop_duplicates()


def create_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    """Create product dimension table"""
    logger.info("Creating product dimension table")
    dim_product = df[["product_id", "brand", "price", "price_tier"]].copy()
    dim_product.loc[:, "product_id"] = dim_product["product_id"].astype(int)
    dim_product.loc[:, "price"] = dim_product["price"].astype(float)
    return dim_product.drop_duplicates()


def create_dim_category(df: pd.DataFrame) -> pd.DataFrame:
    """Create category dimension table"""
    logger.info("Creating category dimension table")
    dim_category = df[
        ["category_id", "category_code", "category_l1", "category_l2", "category_l3"]
    ].copy()
    dim_category.loc[:, "category_id"] = dim_category["category_id"].astype(int)
    return dim_category.drop_duplicates()


def create_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """Create date dimension table"""
    logger.info("Creating date dimension table")
    dim_date = df[["event_date", "event_hour", "day_of_week"]].copy()
    return dim_date.drop_duplicates()


def create_fact_events(df: pd.DataFrame, dims: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Create fact table with foreign keys to dimensions"""
    logger.info("Creating fact events table")
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
    ].copy()

    fact_events.loc[:, "user_id"] = fact_events["user_id"].astype(int)
    fact_events.loc[:, "product_id"] = fact_events["product_id"].astype(int)
    fact_events.loc[:, "category_id"] = fact_events["category_id"].astype(int)
    fact_events.loc[:, "events_in_session"] = fact_events["events_in_session"].astype(
        int
    )

    return fact_events


@task()
def load_dimensions_and_facts(transformed_data: Dict[str, Any]) -> bool:
    """Load dimensional model into Data Warehouse"""
    logger.info("Loading dimensions and facts into Data Warehouse")
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")
        df = pd.DataFrame(transformed_data["data"])

        # Create schema if not exists
        postgres_hook.run("CREATE SCHEMA IF NOT EXISTS dwh;")

        # Create dimensions
        dims = {
            "dwh.dim_user": create_dim_user(df),
            "dwh.dim_product": create_dim_product(df),
            "dwh.dim_category": create_dim_category(df),
            "dwh.dim_date": create_dim_date(df),
        }

        # Map table names to schema classes
        schema_mapping = {
            "dwh.dim_user": DimUserSchema,
            "dwh.dim_product": DimProductSchema,
            "dwh.dim_category": DimCategorySchema,
            "dwh.dim_date": DimDateSchema,
        }

        # Create fact table
        fact_events = create_fact_events(df, dims)

        # Create and load dimension tables
        for table_name, dim_df in dims.items():
            schema_class = schema_mapping[table_name]
            create_schema_and_table(postgres_hook, schema_class, table_name)
            batch_insert_data(postgres_hook, dim_df, table_name)

        # Create and load fact table
        create_schema_and_table(postgres_hook, FactEventSchema, "dwh.fact_events")
        batch_insert_data(postgres_hook, fact_events, "dwh.fact_events")

        # Create useful views
        create_analytical_views(postgres_hook)

        return True

    except Exception as e:
        logger.error(f"Failed to load dimensional model: {str(e)}")
        raise Exception(f"Failed to load dimensional model: {str(e)}")


def create_analytical_views(postgres_hook: PostgresHook) -> None:
    """Create useful views for analysis"""
    logger.info("Creating analytical views")
    try:
        # Load and execute SQL file
        sql = load_sql_template("views/analytical_views.sql")
        postgres_hook.run(sql)

        # Verify views exist
        views = [
            "vw_user_session_summary",
            "vw_category_performance",
            "vw_ml_purchase_prediction",
        ]
        for view_name in views:
            verification_sql = f"""
            SELECT EXISTS (
                SELECT FROM pg_views
                WHERE schemaname = 'dwh'
                AND viewname = '{view_name}'
            );
            """
            exists = postgres_hook.get_first(verification_sql)[0]
            if exists:
                logger.info(f"Successfully verified view exists: {view_name}")
            else:
                logger.error(f"View was not created: {view_name}")

    except Exception as e:
        logger.error(f"Failed to create analytical views: {str(e)}")
        raise Exception(f"Failed to create analytical views: {str(e)}")
