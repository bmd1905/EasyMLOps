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
