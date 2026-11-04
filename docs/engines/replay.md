# Quarantine Replay Engine (`dlt-doctor replay`)

Once records have been quarantined and corrected (either via manual remediation in an operational data store, upstream source fixes, or schema evolution), data engineers need an idempotent mechanism to merge resolved records into target tables.

## Pipeline-Managed Tables vs Re-feeding Upstream

> [!IMPORTANT]
> **DML Restrictions on Pipeline-Managed Tables:**
> Direct DML operations (`MERGE`, `UPDATE`, `DELETE`) against streaming tables or materialized views managed by Databricks Lakeflow / Delta Live Tables are restricted in Unity Catalog. External DML bypasses pipeline checkpointing, state stores, and dependency graphs, risking state corruption or silent overwrites on subsequent pipeline updates.

### Recommended Pattern: Upstream Re-feeding
The most robust way to reprocess quarantined records is to **re-feed corrected rows into the pipeline's upstream ingestion source** (such as the Bronze table or landing stream) with an incremented timestamp or sequence ID. The declarative pipeline will then ingest and validate the records naturally through its standard lineage.

### Secondary Pattern: Delta MERGE Replay
For standalone Delta Lake target tables, staging layers, or unmanaged pipelines, `dlt-doctor replay` generates deduplicated, NULL-safe MERGE SQL with automated recency guards.

## The Replay Workflow

1. **Diagnosis**: Query the quarantine table to identify failing rules:
   ```sql
   SELECT explode(_failed_rules) AS failed_rule, count(*)
   FROM main.silver.orders_quarantine
   GROUP BY 1;
   ```

2. **Generate Replay SQL**: Use `dlt-doctor replay`:
   ```bash
   dlt-doctor replay \
     --quarantine main.silver.orders_quarantine \
     --target main.silver.orders \
     --keys order_id \
     --rule positive_amount \
     --start-time 2026-10-01T00:00:00Z \
     --recency-col updated_at
   ```

3. **Generated SQL Pattern**:
   ```sql
   -- Replay SQL: Reprocess quarantined records into target table idempotently
   -- Note: Prefer re-feeding corrected rows into the upstream source stream rather than direct DML.
   MERGE INTO main.silver.orders AS target
   USING (
       SELECT * EXCEPT (_failed_rules, _quarantined_at),
              current_timestamp() AS _replayed_at
       FROM main.silver.orders_quarantine
       WHERE array_contains(_failed_rules, 'positive_amount') AND _quarantined_at >= '2026-10-01T00:00:00Z'
       QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _quarantined_at DESC) = 1
   ) AS source
   ON target.order_id = source.order_id
   WHEN MATCHED AND (target.updated_at IS NULL OR source.updated_at >= target.updated_at) THEN UPDATE SET *
   WHEN NOT MATCHED THEN INSERT *;
   ```

## Idempotence and Safeguards

- **Deduplication by Key**: The generated query includes `QUALIFY row_number() OVER (PARTITION BY ... ORDER BY _quarantined_at DESC) = 1` to ensure multiple quarantine occurrences for the same key do not trigger Delta MERGE multiple-match exceptions.
- **ISO Timestamp Validation**: CLI parameters `--start-time` and `--end-time` are validated against ISO 8601 to prevent malformed SQL predicates.
- **Recency Guard**: Older quarantined rows cannot overwrite newer target records (`target.recency_col IS NULL OR source.recency_col >= target.recency_col`).
- **Audit Columns**: Every replayed row is marked with `_replayed_at = current_timestamp()`.
