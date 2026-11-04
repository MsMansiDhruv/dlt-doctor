# CLI & Usage Guide

`dlt-doctor` provides a unified command-line tool with 12 specialized diagnostic engines.

## Essential Commands

### 1. Repository & Directory Auditing
Recursively audits all `.py` pipelines and `.ipynb` notebooks in a directory:
```bash
dlt-doctor analyze src/pipelines --recursive
```

Output format can be adjusted for terminal, markdown, or HTML:
```bash
dlt-doctor analyze src/pipelines --format markdown
```

### 2. Single Pipeline or Notebook Audit
```bash
# Audit a Python pipeline
dlt-doctor analyze src/pipelines/orders_silver.py

# Audit a Databricks notebook
dlt-doctor analyze notebooks/01_ingest.ipynb
```

### 3. Late-Arriving Data Restatement
Calculates bounded partition blast radius and outputs surgical Delta `MERGE` SQL:
```bash
dlt-doctor restate --table main.gold.orders --late-date 2026-10-25 --window 7
```

### 4. Streaming CDC Deduplication & SCD Merge
Generates deterministic in-batch deduplication logic and idempotency MERGE SQL:
```bash
dlt-doctor cdc --table main.silver.customers --keys customer_id --event-time updated_at
```

### 5. Unity Catalog Migration Audit
Scans for legacy Hive Metastore references and deprecated DBFS mounts (`/mnt/`):
```bash
dlt-doctor uc-migrate src/pipelines/orders.py --catalog prod_lakehouse --schema bronze
```

### 6. Liquid Clustering Advisor
Evaluates column distinctness and query filter weights to advise 1-4 `CLUSTER BY` keys:
```bash
dlt-doctor cluster --table main.gold.orders --cols customer_id:25000:0.9 order_date:1800:0.95 --total-rows 10000000
```

### 7. Delta VACUUM Retention Safety Guard
Verifies retention hours against active streaming readers and Change Data Feed:
```bash
dlt-doctor vacuum --table main.silver.orders --retention-hours 24 --readers 2 --cdf
```

### 8. Databricks DBU Cost & Sizing Forecast
Compares Serverless DLT against Classic Compute pools based on landing file velocity:
```bash
dlt-doctor cost --files-per-hour 25000 --avg-file-mb 12 --joins 2
```

### 9. Shuffle Spill Partition Tuning
Calculates optimal `spark.sql.shuffle.partitions` to prevent disk spills:
```bash
dlt-doctor spill --dataset-gb 50.0 --cores 16
```

### 10. Live Table Telemetry Inspection
Connects to Databricks SQL warehouse to audit fragmentation debt and commit velocity:
```bash
dlt-doctor inspect-table main.gold.orders --warehouse-id <warehouse_id>
```
