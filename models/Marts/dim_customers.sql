{{
    config(
        materialized='table'
    )
}}
WITH customer_metrics AS (
    SELECT
        customer_id,
        COUNT(DISTINCT transaction_id) as total_orders,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_order_value,
        MIN(transaction_date) as first_order_date,
        MAX(transaction_date) as last_order_date
    FROM {{ ref('stg_sales_transactions') }}
    GROUP BY customer_id
),

customer_segments AS (
    SELECT
        *,
        CASE 
            WHEN total_revenue >= 200 THEN 'High Value'
            WHEN total_revenue >= 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END as value_segment
    FROM customer_metrics
)

SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.registration_date,
    c.customer_tier,
    c.city,
    c.state,
    COALESCE(m.total_orders, 0) as total_orders,
    COALESCE(m.total_revenue, 0) as total_revenue,
    COALESCE(m.avg_order_value, 0) as avg_order_value,
    m.first_order_date,
    m.last_order_date,
    COALESCE(m.value_segment, 'No Orders') as value_segment,
    CURRENT_TIMESTAMP() as _updated_at
FROM {{ ref('stg_customers') }} c
LEFT JOIN customer_segments m ON c.customer_id = m.customer_id