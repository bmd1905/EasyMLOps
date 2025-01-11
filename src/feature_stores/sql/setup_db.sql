-- Create user entity table
CREATE TABLE IF NOT EXISTS dwh.entity_user (
    user_id BIGINT PRIMARY KEY,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create product entity table
CREATE TABLE IF NOT EXISTS dwh.entity_product (
    product_id BIGINT PRIMARY KEY,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add table for offline store if not exists
CREATE TABLE IF NOT EXISTS feast_feature_store (
    entity_key VARCHAR(255),
    feature_name VARCHAR(255),
    value JSONB,
    event_timestamp TIMESTAMP,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_key, feature_name, event_timestamp)
);
