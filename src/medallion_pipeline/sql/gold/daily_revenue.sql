-- Grain: one row per calendar day (UTC) with recognised revenue.
WITH recognised AS (
    SELECT
        o.order_id,
        CAST(o.order_ts AS DATE) AS order_date,
        i.quantity,
        i.line_total
    FROM silver_orders AS o
    INNER JOIN silver_order_items AS i ON o.order_id = i.order_id
    WHERE o.status IN ({{ revenue_statuses }})
)

SELECT
    order_date,
    COUNT(DISTINCT order_id) AS orders,
    CAST(SUM(quantity) AS BIGINT) AS units_sold,
    CAST(SUM(line_total) AS DECIMAL(16, 2)) AS gross_revenue,
    CAST(SUM(line_total) / COUNT(DISTINCT order_id) AS DECIMAL(16, 2)) AS avg_order_value
FROM recognised
GROUP BY order_date
ORDER BY order_date
