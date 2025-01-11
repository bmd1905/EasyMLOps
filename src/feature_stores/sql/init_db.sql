-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS feature_store;

--- Drop views if exists
DROP VIEW IF EXISTS feature_store.ml_purchase_prediction;

-- Create view for ML purchase prediction
CREATE OR REPLACE VIEW feature_store.ml_purchase_prediction AS
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

-- Create user entity table
CREATE TABLE IF NOT EXISTS feature_store.entity_user (
    user_id BIGINT PRIMARY KEY,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create product entity table
CREATE TABLE IF NOT EXISTS feature_store.entity_product (
    product_id BIGINT PRIMARY KEY,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add table for offline store if not exists
CREATE TABLE IF NOT EXISTS feature_store.offline_store (
    entity_key VARCHAR(255),
    feature_name VARCHAR(255),
    value JSONB,
    event_timestamp TIMESTAMP,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_key, feature_name, event_timestamp)
);
