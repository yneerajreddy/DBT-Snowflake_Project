{{
    config(
        materialized='view'
    )
}}
SELECT
    transaction_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price as total_amount,
    transaction_date,
    payment_method,
    store_location,
    _loaded_at
FROM {{ source('raw_data', 'raw_sales_transactions') }}
WHERE transaction_date IS NOT NULL
  AND quantity > 0
  AND unit_price > 0