# Late Data Restatement (`RestatementDoctor`)

When late-arriving records enter historical rolling windows (e.g. 7-day or 30-day aggregations), engineers often reload months or years of historical partitions.

## Bounded Blast Radius

`RestatementDoctor` calculates the exact earliest affected partition date and generates surgical partition-pruned Delta `MERGE` SQL, reducing I/O scan by **up to 98.1%**.

## Python API Usage

```python
from dlt_doctor import RestatementDoctor

reconciler = RestatementDoctor()
scope = reconciler.calculate_blast_radius(
    target_table="main.gold.fact_orders",
    late_date_iso="2026-10-25",
    window_size_days=7,
    partition_col="order_date",
    match_key="order_id",
)

print(f"Blast Radius: {scope.blast_radius_start} to {scope.blast_radius_end}")
print(scope.delta_merge_sql)
```
