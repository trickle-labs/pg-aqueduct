-- models/order_totals.sql
-- dbt model: aggregate order amounts per customer
-- config(materialized='stream_table', schedule='30s', refresh_mode='DIFFERENTIAL')
SELECT
    customer_id,
    SUM(amount) AS total_amount
FROM {{ source('warehouse', 'orders') }}
GROUP BY customer_id
