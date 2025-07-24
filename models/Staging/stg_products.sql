{{
    config(
        materialized='view'
    )
}}
SELECT
    product_id,
    product_name,
    category,
    brand,
    unit_cost,
    retail_price,
    retail_price - unit_cost as profit_margin,
    stock_quantity,
    _loaded_at
FROM {{ source('raw_data', 'raw_products') }}
WHERE product_id IS NOT NULL