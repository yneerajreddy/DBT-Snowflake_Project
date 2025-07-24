{{
    config(
        materialized='view'
    )
}}

SELECT
    review_id,
    product_id,
    customer_id,
    rating,
    review_text,
    review_date,
    _loaded_at
FROM {{ source('raw_data', 'raw_reviews') }}
WHERE review_id IS NOT NULL
  AND rating BETWEEN 1 AND 5
