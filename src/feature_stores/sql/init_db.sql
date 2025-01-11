-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS dwh;

--- Drop views if exists
DROP VIEW IF EXISTS dwh.vw_ml_purchase_prediction;

-- Create view for ML purchase prediction
CREATE OR REPLACE VIEW dwh.vw_ml_purchase_prediction AS
SELECT
    user_id,
    product_id,
    event_timestamp,
    price,
    category_code_level1,
    category_code_level2,
    brand,
    activity_count,
    event_weekday,
    is_purchased,
    user_session
FROM (
    -- Placeholder select to avoid empty view
    SELECT
        0::bigint as user_id,
        0::bigint as product_id,
        CURRENT_TIMESTAMP as event_timestamp,
        0.0::float as price,
        ''::text as category_code_level1,
        ''::text as category_code_level2,
        ''::text as brand,
        0::bigint as activity_count,
        0::int as event_weekday,
        0::int as is_purchased,
        ''::text as user_session
    WHERE FALSE
) t;
