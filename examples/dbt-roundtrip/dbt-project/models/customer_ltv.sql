-- models/customer_ltv.sql
-- dbt model: customer lifetime value derived from order totals
-- config(materialized='stream_table', schedule='5m', refresh_mode='DIFFERENTIAL')
SELECT
    ot.customer_id,
    ot.total_amount AS lifetime_value
FROM {{ ref('order_totals') }} ot
