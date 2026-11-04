# Liquid Clustering Advisor (`ClusterDoctor`)

Databricks Liquid Clustering (`CLUSTER BY`) replaces legacy table partitioning and Z-Ordering. Selecting improper clustering keys (such as near-unique UUIDs) degrades compaction without improving query pruning.

## Cardinality & Filter Scoring

`ClusterDoctor` profiles column distinctness and query filter frequencies:
- **Accepts**: High-selectivity filter columns with moderate cardinality (e.g. `customer_id`, `order_date`).
- **Rejects**: Near-unique UUIDs (distinctness > 80% of total rows) or low-cardinality flags.
- **Bounds**: Restricts clustering to 1–4 columns per Databricks best practices.

## Python API Usage

```python
from dlt_doctor import ClusterDoctor, ColumnProfile

advisor = ClusterDoctor()
rec = advisor.advise_clustering(
    table_name="main.gold.fact_orders",
    columns=[
        ColumnProfile("customer_id", "string", distinct_count=25000, filter_frequency_score=0.9),
        ColumnProfile("order_date", "date", distinct_count=1800, filter_frequency_score=0.95),
        ColumnProfile("order_uuid", "string", distinct_count=9900000),  # Rejected: near-unique
    ],
    total_rows=10000000,
)

print(rec.alter_cluster_ddl)
# ALTER TABLE main.gold.fact_orders CLUSTER BY (customer_id, order_date);
```
