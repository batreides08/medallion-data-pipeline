-- Grain: one row per customer, including customers without recognised revenue.
WITH order_totals AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_ts,
        SUM(i.line_total) AS order_total
    FROM silver_orders AS o
    INNER JOIN silver_order_items AS i ON o.order_id = i.order_id
    WHERE o.status IN ({{ revenue_statuses }})
    GROUP BY o.order_id, o.customer_id, o.order_ts
),

per_customer AS (
    SELECT
        customer_id,
        COUNT(*) AS orders,
        MIN(order_ts) AS first_order_ts,
        MAX(order_ts) AS last_order_ts,
        SUM(order_total) AS lifetime_revenue
    FROM order_totals
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.country,
    COALESCE(p.orders, 0) AS orders,
    p.first_order_ts,
    p.last_order_ts,
    CAST(COALESCE(p.lifetime_revenue, 0) AS DECIMAL(16, 2)) AS lifetime_revenue,
    CASE
        WHEN COALESCE(p.lifetime_revenue, 0) >= 5000 THEN 'vip'
        WHEN COALESCE(p.lifetime_revenue, 0) >= 1000 THEN 'regular'
        WHEN COALESCE(p.lifetime_revenue, 0) > 0 THEN 'occasional'
        ELSE 'inactive'
    END AS value_segment
FROM silver_customers AS c
LEFT JOIN per_customer AS p ON c.customer_id = p.customer_id
