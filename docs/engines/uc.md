# Unity Catalog Migration (`UcDoctor`)

`UcDoctor` audits legacy Databricks code for deprecated Hive Metastore references, hardcoded DBFS mounts (`/mnt/...`), and single/two-tier namespaces.

## Capabilities

- Detects `/mnt/` mount points and translates to Unity Catalog Volumes (`/Volumes/catalog/schema/volume/...`).
- Validates 3-tier namespaces (`catalog.schema.table`).
- Generates migration DDL and path remapping dictionaries.

## Python API Usage

```python
from dlt_doctor import UcDoctor

doctor = UcDoctor()
plan = doctor.audit_script_migrations(
    source_code="""
df = spark.read.parquet("/mnt/landing/orders")
df.write.saveAsTable("bronze_orders")
""",
    target_catalog="prod_lakehouse",
    target_schema="bronze",
)

for finding in plan.findings:
    print(f"[{finding.severity.value}] {finding.message} -> {finding.suggested_replacement}")
```
