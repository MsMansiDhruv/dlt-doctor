# Quarantine Governance Architecture

In regulated enterprise pipelines, data quality failures must never result in silent record loss or unmonitored corruption. `dlt-doctor` enforces a zero-loss quarantine architecture built on strict SQL three-valued logic (3VL) completeness and single-pass execution.

## The Three-Valued Logic (3VL) Completeness Guarantee

Standard SQL predicates evaluate to `TRUE`, `FALSE`, or `NULL` (Unknown). Naive dual-filtering constructs:

```python
valid_filter = "amount > 0"
invalid_filter = "NOT (amount > 0)"
```

suffer from catastrophic data loss when `amount` is `NULL`:

1. `(NULL > 0)` evaluates to `NULL`.
2. `NOT (NULL)` evaluates to `NULL`.
3. In SQL `WHERE` and `.filter()`, expressions that evaluate to `NULL` are discarded.

As a result, rows with `amount IS NULL` are dropped from **both** the clean table and the quarantine table.

### The Mathematical Fix

`dlt-doctor` wraps every condition in `coalesce((<condition>), false)`:

```sql
-- Valid filter (Complete)
coalesce((amount > 0), false)

-- Invalid filter (Disjoint Complement)
NOT (coalesce((amount > 0), false))
```

This guarantees:

$$\text{Valid} \cup \text{Quarantine} = \text{Input}$$
$$\text{Valid} \cap \text{Quarantine} = \emptyset$$

Every input row is guaranteed to land in exactly one destination.

## Single-Pass Evaluation Architecture

Evaluating expectations independently on two streaming queries causes double ingestion and stream re-reads. `dlt-doctor` synthesizes a single-pass intermediate view:

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.temporary_view()
def silver_orders_evaluated():
    source_df = dp.read_stream("source_bronze")
    return source_df.select(
        "*",
        F.array_compact(
            F.array(
                F.when(~F.expr("coalesce((amount > 0), false)"), F.lit("positive_amount"))
            )
        ).alias("_failed_rules"),
        F.current_timestamp().alias("_quarantined_at"),
    )

@dp.table(name="silver_orders")
def silver_orders():
    return (
        dp.read_stream("silver_orders_evaluated")
        .filter("size(_failed_rules) = 0")
        .drop("_failed_rules", "_quarantined_at")
    )

@dp.table(name="silver_orders_quarantine")
def silver_orders_quarantine():
    return (
        dp.read_stream("silver_orders_evaluated")
        .filter("size(_failed_rules) > 0")
    )
```

## Quarantine Table Governance

Quarantined tables must adhere to standard governance lifecycle requirements:

1. **Schema Preservation**: Quarantine tables retain the full original record payload alongside `_failed_rules` (array of string rule names) and `_quarantined_at` (audit timestamp).
2. **Access Control**: Quarantine tables should reside in Unity Catalog schemas with restricted data steward access.
3. **Audit Alerts**: Monitor row counts on quarantine streams to detect upstream schema drift or ingestion anomalies.
4. **Replay Reconciliation**: Remediate underlying data errors upstream or execute surgical replay using `dlt-doctor replay`.
