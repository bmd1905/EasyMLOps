# This is an example feature definition file
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)

# Source for user features
user_source = PostgreSQLSource(
    name="user_features_source",
    query="""
    SELECT
        user_id,
        event_count as activity_count,
        unique_products_viewed,
        session_start as event_timestamp
    FROM dwh.vw_user_session_summary
    """,
    timestamp_field="event_timestamp",
)

# Source for product features
product_source = PostgreSQLSource(
    name="product_features_source",
    query="""
    SELECT DISTINCT
        f.product_id,
        p.price,
        c.category_l1 as category_code_level1,
        c.category_l2 as category_code_level2,
        f.event_timestamp
    FROM dwh.fact_events f
    JOIN dwh.dim_product p ON f.product_id = p.product_id
    JOIN dwh.dim_category c ON f.category_id = c.category_id
    WHERE c.category_l1 IS NOT NULL
        AND c.category_l2 IS NOT NULL
        AND f.event_type IN ('cart', 'purchase')
    """,
    timestamp_field="event_timestamp",
)
