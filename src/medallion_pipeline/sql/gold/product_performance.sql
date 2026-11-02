-- Grain: one row per product, ranked by revenue inside its category.
WITH sales AS (
    SELECT
        i.product_id,
        SUM(i.quantity) AS units_sold,
        SUM(i.line_total) AS revenue
    FROM silver_order_items AS i
    INNER JOIN silver_orders AS o ON i.order_id = o.order_id
    WHERE o.status IN ({{ revenue_statuses }})
    GROUP BY i.product_id
)

SELECT
    p.product_id,
    p.name,
    p.category,
    CAST(COALESCE(s.units_sold, 0) AS BIGINT) AS units_sold,
    CAST(COALESCE(s.revenue, 0) AS DECIMAL(16, 2)) AS revenue,
    RANK() OVER (
        PARTITION BY p.category
        ORDER BY COALESCE(s.revenue, 0) DESC
    ) AS category_revenue_rank
FROM silver_products AS p
LEFT JOIN sales AS s ON p.product_id = s.product_id
