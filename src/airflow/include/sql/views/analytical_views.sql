-- Supporting indexes
CREATE INDEX IF NOT EXISTS idx_fact_events_cart_purchase
ON dwh.fact_events (event_type, user_session, product_id)
WHERE event_type IN ('cart', 'purchase');

CREATE INDEX IF NOT EXISTS idx_fact_events_session_product
ON dwh.fact_events (user_session, product_id);

-- User session summary view
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
GROUP BY u.user_id, f.user_session;

-- Category performance view
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
GROUP BY c.category_l1, c.category_l2, c.category_l3;

-- ML purchase prediction view
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
    AND c.category_l2 IS NOT NULL;
