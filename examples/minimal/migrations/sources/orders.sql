-- @aqueduct:kind    = "source"
-- @aqueduct:owned  = false
-- @aqueduct:schema = "raw"
CREATE TABLE raw.orders (
    id          bigint      PRIMARY KEY,
    customer_id bigint      NOT NULL,
    amount      numeric(12,2) NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
