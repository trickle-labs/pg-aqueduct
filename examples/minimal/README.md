# Minimal Example — 3-Node DAG

This example demonstrates a simple 3-node stream-table DAG:

```
raw.orders (source)
    │
    ▼
public.order_totals     (DIFFERENTIAL, GROUP BY customer_id)
    │
    ▼
public.customer_tiers   (DIFFERENTIAL, tier classification)
```

## Usage

```bash
# Set up your database connection.
export AQUEDUCT_DEV_DSN="postgresql://localhost/mydb"

# Bootstrap the catalog (first time only).
aqueduct init --to dev

# See what would be applied.
aqueduct plan --to dev

# Apply the changes.
aqueduct apply --to dev

# Check status.
aqueduct status --to dev
```
