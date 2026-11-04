# Workspace Telemetry (`WorkspaceInspector`)

`WorkspaceInspector` connects directly to a live Databricks workspace via the SQL Statements API to inspect actual Delta table metadata without pulling raw data rows.

## Telemetry Audited

- Total file count and average file size in MB.
- Current Liquid Clustering keys and partition columns.
- Transaction commit velocity in the past 24 hours.
- Compaction debt status and generated `OPTIMIZE ... CLUSTER BY` SQL.
- Streaming reader retention safety and VACUUM recommendations.

## Python API Usage

```python
from dlt_doctor import WorkspaceInspector

inspector = WorkspaceInspector(
    host="https://adb-123456789.azuredatabricks.net",
    token="dapi...",
    warehouse_id="1234abcd5678efgh",
)

report = inspector.inspect_table("main.gold.fact_orders")
print(report.to_text())
```
