-- Test that all transactions have positive revenue
SELECT transaction_id
FROM {{ ref('fct_sales') }}
WHERE total_amount <= 0