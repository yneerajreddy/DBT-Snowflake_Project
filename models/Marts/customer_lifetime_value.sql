{{ config(materialized='view', schema='analytics') }}

SELECT
    customer_id,
    full_name as customer_name,
    customer_tier,
    city,
    state,
    total_orders,
    total_revenue,
    avg_order_value,
    value_segment,
    first_order_date,
    last_order_date,
    DATEDIFF(day, first_order_date, last_order_date) as customer_lifespan_days,
    CASE 
        WHEN last_order_date >= DATEADD(day, -30, CURRENT_DATE()) THEN 'Active'
        WHEN last_order_date >= DATEADD(day, -90, CURRENT_DATE()) THEN 'At Risk'
        ELSE 'Inactive'
    END as customer_status
FROM {{ ref('dim_customers') }}
WHERE total_orders > 0
ORDER BY total_revenue DESC