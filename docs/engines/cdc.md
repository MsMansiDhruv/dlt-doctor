# Streaming CDC Deduplication (`CdcDoctor`)

Streaming Change Data Capture (CDC) pipelines crash with `DeltaAnalysisException: Cannot perform Merge as multiple source rows matched` when duplicate keys arrive within the same streaming micro-batch.

## Deterministic Micro-Batch Windowing

`CdcDoctor` generates pre-merge window deduplication logic using `row_number() OVER (PARTITION BY primary_keys ORDER BY event_time DESC)` and deterministic SCD Type 1 & Type 2 `MERGE` SQL templates.

## Python API Usage

```python
from dlt_doctor import CdcDoctor

cdc = CdcDoctor().generate_cdc_templates(
    target_table="main.silver.customers",
    primary_keys=["customer_id"],
    event_time_col="updated_at",
)

# Used inside foreachBatch handler
print("Micro-batch Deduplication Logic:")
print(cdc.microbatch_dedup_code)

print("\nDeterministic MERGE SQL:")
print(cdc.scd1_merge_sql)
```
