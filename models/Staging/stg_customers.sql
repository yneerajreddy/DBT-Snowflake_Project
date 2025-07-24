{{
    config(
        materialized='view'
    )
}}
SELECT
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email,
    registration_date,
    customer_tier,
    city,
    state,
    _loaded_at
FROM {{ source('raw_data', 'raw_customers') }}
WHERE customer_id IS NOT NULL