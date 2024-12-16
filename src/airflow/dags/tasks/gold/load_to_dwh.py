from typing import Any, Dict

import pandas as pd
from common.scripts.db_utils import batch_insert_data, create_schema_and_table
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
        # Create supporting indexes first
        supporting_indexes = [
            """
            CREATE INDEX IF NOT EXISTS idx_fact_events_cart_purchase
            ON dwh.fact_events (event_type, user_session, product_id)
            WHERE event_type IN ('cart', 'purchase');
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_fact_events_session_product
            ON dwh.fact_events (user_session, product_id);
            """,
        ]

        for index_sql in supporting_indexes:
            try:
                postgres_hook.run(index_sql)
                logger.info(f"Created supporting index: {index_sql[:50]}...")
            except Exception as e:
                logger.error(f"Failed to create supporting index: {str(e)}")
                raise

        views = {
            "dwh.vw_user_session_summary": """
                CREATE OR REPLACE VIEW dwh.vw_user_session_summary AS
                SELECT
                    u.user_id,
                    f.user_session,
                    COUNT(*) as event_count,
                    MIN(f.event_timestamp) as session_start,
                    MAX(f.event_timestamp) as session_end,
                    COUNT(DISTINCT p.product_id) as unique_products_viewed
                FROM dwh.fact_events f
                JOIN dwh.dim_user u ON f.user_id = u.user_id
                JOIN dwh.dim_date d ON f.event_date = d.event_date
                JOIN dwh.dim_product p ON f.product_id = p.product_id
                GROUP BY u.user_id, f.user_session
            """,
            "dwh.vw_category_performance": """
                CREATE OR REPLACE VIEW dwh.vw_category_performance AS
                SELECT
                    c.category_l1,
                    c.category_l2,
                    c.category_l3,
                    COUNT(*) as view_count,
                    COUNT(DISTINCT u.user_id) as unique_users,
                    AVG(p.price) as avg_price
                FROM dwh.fact_events f
                JOIN dwh.dim_category c ON f.category_id = c.category_id
                JOIN dwh.dim_user u ON f.user_id = u.user_id
                JOIN dwh.dim_product p ON f.product_id = p.product_id
                GROUP BY c.category_l1, c.category_l2, c.category_l3
            """,
            "dwh.vw_ml_purchase_prediction": """
                DROP VIEW IF EXISTS dwh.vw_ml_purchase_prediction;
                CREATE VIEW dwh.vw_ml_purchase_prediction AS
                WITH cart_purchase_events AS (
                    SELECT DISTINCT
                        f.event_type,
                        f.product_id,
                        f.user_id,
                        f.user_session,
                        f.event_timestamp,
                        f.category_id,
                        p.price,
                        p.brand
                    FROM dwh.fact_events f
                    JOIN dwh.dim_product p ON f.product_id = p.product_id
                    WHERE f.event_type IN ('cart', 'purchase')
                ),
                purchase_flags AS (
                    SELECT
                        user_session,
                        product_id,
                        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as is_purchased
                    FROM cart_purchase_events
                    GROUP BY user_session, product_id
                ),
                session_activity_counts AS (
                    SELECT
                        user_session,
                        COUNT(*) as activity_count
                    FROM dwh.fact_events
                    GROUP BY user_session
                )
                SELECT DISTINCT
                    cp.user_session,
                    cp.product_id,
                    cp.price,
                    cp.user_id,
                    cp.brand,
                    EXTRACT(DOW FROM cp.event_timestamp) as event_weekday,
                    c.category_l1 as category_code_level1,
                    c.category_l2 as category_code_level2,
                    sa.activity_count,
                    pf.is_purchased
                FROM cart_purchase_events cp
                JOIN purchase_flags pf
                    ON cp.user_session = pf.user_session
                    AND cp.product_id = pf.product_id
                JOIN dwh.dim_category c
                    ON cp.category_id = c.category_id
                LEFT JOIN session_activity_counts sa
                    ON cp.user_session = sa.user_session
                WHERE c.category_l1 IS NOT NULL
                    AND c.category_l2 IS NOT NULL
            """,
        }

        for view_name, view_sql in views.items():
            logger.info(f"Creating view: {view_name}")
            try:
                postgres_hook.run(view_sql)

                # Verify view exists
                verification_sql = f"""
                SELECT EXISTS (
                    SELECT FROM pg_views
                    WHERE schemaname = 'dwh'
                    AND viewname = '{view_name.replace("dwh.", "")}'
                );
                """
                exists = postgres_hook.get_first(verification_sql)[0]
                if exists:
                    logger.info(f"Successfully verified view exists: {view_name}")
                else:
                    logger.error(f"View was not created: {view_name}")

            except Exception as e:
                logger.error(f"Failed to create view {view_name}: {str(e)}")
                raise

    except Exception as e:
        logger.error(f"Failed to create analytical views: {str(e)}")
        raise Exception(f"Failed to create analytical views: {str(e)}")
