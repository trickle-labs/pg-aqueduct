SELECT
    ot.customer_id,
    ot.total_amount AS lifetime_value
FROM public.order_totals ot
