# Quarantine Replay Engine (`dlt-doctor replay`)

Once records have been quarantined and corrected (either via manual remediation in an operational data store, upstream source fixes, or schema evolution), data engineers need an idempotent mechanism to merge resolved records into the target production tables.

## The Replay Workflow

1. **Diagnosis**: Query the quarantine table to identify failing rules:
   ```sql
   SELECT explode(_failed_rules) AS failed_rule, count(*)
   FROM main.silver.orders_quarantine
   GROUP BY 1;
   ```

2. **Generate Replay SQL**: Use `dlt-doctor replay` to generate idempotent Delta Lake MERGE SQL:
   ```bash
   dlt-doctor replay \
     --quarantine-table main.silver.orders_quarantine \
     --target-table main.silver.orders \
     --keys order_id \
     --filter "order_date >= '2026-10-01'"
   ```

3. **Generated SQL Pattern**:
   ```sql
   MERGE INTO main.silver.orders AS target
   USING (
     SELECT * EXCEPT (_failed_rules, _quarantined_at)
     FROM main.silver.orders_quarantine
     WHERE order_date >= '2026-10-01'
   ) AS source
   ON target.order_id = source.order_id
   WHEN MATCHED THEN UPDATE SET *
   WHEN NOT MATCHED THEN INSERT *;
   ```

## Idempotence and Auditability

- **Deduplication**: If multiple records exist for the same primary key in the quarantine stream, deduplicate within a staging view prior to MERGE execution.
- **Audit Column Stripping**: Helper columns (`_failed_rules`, `_quarantined_at`) are automatically excluded from the source dataset so they do not collide with target schemas.
- **Tombstoning**: Following successful replay, quarantined rows can either be deleted or flagged with a replay status (`_is_replayed = true`, `_replayed_at = current_timestamp()`).
