SELECT
    promo_code,
    COUNT(*) AS redemption_count
FROM raw.promos
GROUP BY promo_code
