-- @aqueduct:schedule     = "{{ var.schedule }}"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
-- @aqueduct:depends_on   = ["public.order_totals"]
SELECT
    o.customer_id,
    ot.total_amount,
    ot.order_count,
    CASE
        WHEN ot.total_amount > 1000 THEN 'high'
        WHEN ot.total_amount > 100  THEN 'medium'
        ELSE 'low'
    END AS customer_tier
FROM public.order_totals ot
JOIN raw.orders o ON o.customer_id = ot.customer_id
GROUP BY o.customer_id, ot.total_amount, ot.order_count;
