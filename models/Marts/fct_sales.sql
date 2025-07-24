{{
    config(
        materialized='table'
    )
}}
SELECT
    s.transaction_id,
    s.customer_id,
    s.product_id,
    c.full_name as customer_name,
    c.customer_tier,
    c.city as customer_city,
    c.state as customer_state,
    p.product_name,
    p.category as product_category,
    p.brand as product_brand,
    s.quantity,
    s.unit_price,
    s.total_amount,
    p.unit_cost,
    (s.total_amount - (p.unit_cost * s.quantity)) as profit,
    s.transaction_date,
    s.payment_method,
    s.store_location,
    CURRENT_TIMESTAMP() as _updated_at
FROM {{ ref('stg_sales_transactions') }} s
LEFT JOIN {{ ref('stg_customers') }} c ON s.customer_id = c.customer_id
LEFT JOIN {{ ref('stg_products') }} p ON s.product_id = p.product_id