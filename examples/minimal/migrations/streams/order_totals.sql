-- @aqueduct:schedule     = "{{ var.schedule }}"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
-- @aqueduct:depends_on   = ["raw.orders"]
SELECT
    customer_id,
    SUM(amount)  AS total_amount,
    COUNT(*)     AS order_count
FROM raw.orders
GROUP BY customer_id;
