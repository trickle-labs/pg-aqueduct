SELECT
    customer_id,
    SUM(amount) AS total_amount
FROM raw.orders
GROUP BY customer_id
