# Pattern 24 — Create a Consumer View

**Class:** Create (consumer view) | **Test:** `test_cookbook_24_create_consumer_view`

## When to use

Expose a stream table through a stable, schema-qualified view for application or API consumption. The application always connects to the stable view name; the underlying stream table can be replaced transparently.

## Migration file

```sql
-- migrations/consumers/c24_orders_view.sql
-- @aqueduct:kind = consumer
-- @aqueduct:source = public.c24_orders
-- @aqueduct:expose_as = api.orders
SELECT customer_id, total FROM public.c24_orders WHERE total > 0;
```

## Plan output

```text
  + api.orders    [consumer create]   exposes public.c24_orders
```

## Benefits

- Applications connect to `api.orders` — the name never changes.
- On blue/green deployments, the view is swapped atomically with `ACCESS EXCLUSIVE` on the view (not the underlying table), making the cutover sub-millisecond.

## Notes

- Consumer views are tracked in `aqueduct.consumer_views` and listed with `aqueduct consumers list`.
- The `@aqueduct:expose_as` schema must exist before `aqueduct apply` runs. Create it with a pre-hook or Atlas migration.
