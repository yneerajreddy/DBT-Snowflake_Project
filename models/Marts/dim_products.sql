{{
    config(
        materialized='table'
    )
}}WITH product_metrics AS (
    SELECT
        product_id,
        COUNT(DISTINCT transaction_id) as total_transactions,
        SUM(quantity) as total_quantity_sold,
        SUM(total_amount) as total_revenue
    FROM {{ ref('stg_sales_transactions') }}
    GROUP BY product_id
),

review_metrics AS (
    SELECT
        product_id,
        COUNT(*) as total_reviews,
        AVG(rating) as avg_rating,
        COUNT(CASE WHEN rating >= 4 THEN 1 END) as positive_reviews
    FROM {{ ref('stg_reviews') }}
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.unit_cost,
    p.retail_price,
    p.profit_margin,
    p.stock_quantity,
    COALESCE(m.total_transactions, 0) as total_transactions,
    COALESCE(m.total_quantity_sold, 0) as total_quantity_sold,
    COALESCE(m.total_revenue, 0) as total_revenue,
    COALESCE(r.total_reviews, 0) as total_reviews,
    COALESCE(r.avg_rating, 0) as avg_rating,
    COALESCE(r.positive_reviews, 0) as positive_reviews,
    CURRENT_TIMESTAMP() as _updated_at
FROM {{ ref('stg_products') }} p
LEFT JOIN product_metrics m ON p.product_id = m.product_id
LEFT JOIN review_metrics r ON p.product_id = r.product_id