-- models/promo_summary.sql
-- dbt model: promo-code redemption counts
-- config(materialized='stream_table', schedule='1m', refresh_mode='FULL')
SELECT
    promo_code,
    COUNT(*) AS redemption_count
FROM {{ source('warehouse', 'promos') }}
GROUP BY promo_code
