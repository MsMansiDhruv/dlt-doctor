<div align="center">

<img src="https://raw.githubusercontent.com/MsMansiDhruv/dlt-doctor/main/assets/dlt-doctor-mascot.svg" alt="dlt-doctor visual identity" width="100%" />

<p align="center">
  <strong>Production Optimization Engine and Quality Accelerator for Databricks Lakeflow Spark Declarative Pipelines, DLT, and PySpark</strong>
</p>

[![PyPI](https://img.shields.io/badge/PyPI-dlt--doctor-3776AB?style=for-the-badge&logo=pypi&logoColor=white)](https://pypi.org/project/dlt-doctor/)
[![Docs](https://img.shields.io/badge/Docs-Live%20Site-0E1726?style=for-the-badge&logo=materialformkdocs&logoColor=2EE6A7)](https://msmansidhruv.github.io/dlt-doctor/)
[![Runtime](https://img.shields.io/badge/Runtime-Databricks%20%7C%20Spark%203.5+-FF9B4A?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Storage](https://img.shields.io/badge/Storage-Delta%20Lake%20%26%20CDF-2EE6A7?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Governance](https://img.shields.io/badge/Governance-Unity%20Catalog-0E1726?style=for-the-badge&logo=databricks&logoColor=2EE6A7)](https://www.databricks.com/product/unity-catalog)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue?style=for-the-badge)](https://github.com/MsMansiDhruv/dlt-doctor/blob/main/LICENSE)
[![CI](https://github.com/MsMansiDhruv/dlt-doctor/actions/workflows/ci.yml/badge.svg)](https://github.com/MsMansiDhruv/dlt-doctor/actions/workflows/ci.yml)

<p align="center">
  <a href="https://msmansidhruv.github.io/dlt-doctor/">Documentation</a> •
  <a href="#the-story">The Problem</a> •
  <a href="#showcase-video">Video</a> •
  <a href="#benchmarks">Benchmarks</a> •
  <a href="#where-dlt-doctor-fits">Where It Fits</a> •
  <a href="#diagnostic-rules">Diagnostic Rules</a> •
  <a href="#quickstart">Quickstart</a> •
  <a href="#production-walkthrough">Walkthrough</a>
</p>

</div>

> [!NOTE]
> **Beta Release (v0.5.0)**: `dlt-doctor` is in active beta. Interfaces and CLI contracts may evolve prior to v1.0.0. Feedback and contributions are welcome via [GitHub Issues](https://github.com/MsMansiDhruv/dlt-doctor/issues) and [Discussions](https://github.com/MsMansiDhruv/dlt-doctor/discussions).

---

## The Story

Every data engineer working on Databricks Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables) knows the exact feeling:

It is 2 AM. Your streaming pipeline crashes with `DeltaAnalysisException: Cannot perform Merge as multiple source rows matched`. Or worse: native DLT `expect_or_drop` dropped 50,000 regulatory records with no record-level trace, and nobody noticed until audit season. You spin up an interactive cluster to debug, burning DBUs while waiting 4 minutes for workers to allocate, only to discover that someone chained 8 `.withColumn()` calls and wrote a row-level Python `@udf` that disabled Whole-Stage CodeGen and created massive JVM-to-Python socket serialization.

`dlt-doctor` is the antidote:
- **Zero cluster spend**: Statically audits your PySpark AST and configurations locally or inside CI before code merges.
- **Helps prevent silent data loss**: Intercepts destructive `expect_or_drop` policies and synthesizes complete, disjoint, three-valued-logic NULL-safe single-pass quarantine streams via `dlt-doctor heal`.
- **Surgical healing**: Calculates bounded late-data partition blast radii to restate historical rolling windows without full-table scans.

---

## Showcase Video

<div align="center">

<a href="https://github.com/MsMansiDhruv/dlt-doctor/blob/main/assets/dlt-doctor-showcase.mp4">
  <img src="https://raw.githubusercontent.com/MsMansiDhruv/dlt-doctor/main/assets/dlt-doctor-showcase.gif" alt="dlt-doctor product showcase animated preview" width="100%" />
</a>

<p align="center">
  <sub>15-Second Showcase: Eliminate silent drops, accelerate PySpark execution, and shift quality left. Click to view full 1080p video.</sub>
</p>

</div>

---

## Benchmarks

### Static Analyzer Throughput (Locally Measured)

Measured on an AMD Ryzen 7 5800H (16 vCPUs, 3.2GHz, 16GB RAM) running Python 3.11 with zero JVM cluster overhead across 25 production-scale pipeline files:

| Benchmark Task | Workload Scale | Measured Latency | Throughput |
| :--- | :--- | :--- | :--- |
| **Single-Pass Heal** | 10 DLT expectation tables | 4.2 ms | **Sub-5ms per table code synthesis** |
| **Multi-File Directory Scan** | 25 pipeline source files | 35.9 ms | **~700 files/sec** |

---

## Where dlt-doctor Fits

| Architectural Dimension | Native Lakeflow / DLT | DQX / Great Expectations | Commercial SaaS (Monte Carlo / Anomalo) | dlt-doctor |
| :--- | :--- | :--- | :--- | :--- |
| **Cluster Cost** | Standard DLT compute | Executes assertions inside Spark tasks | Queries live tables post-run | **Zero cluster cost** (runs offline via AST in <5ms per file) |
| **Data Loss Prevention** | `expect_or_drop` purges rows without persisting payloads | Supports quarantine routing in DQX (runtime compute); GE typically halts or warns | Read-only alerting after corruption lands | **Single-pass NULL-safe quarantine routing** |
| **Actionability** | Log metrics to event log | Assertion logs | Slack / PagerDuty alert notifications | **Automated code synthesis (`dlt-doctor heal`)** |
| **Code Inspection** | None (runtime engine) | Runtime dataframe assertions | Black-box table polling | **Deep AST inspection (UDFs, cascades, spills)** |
| **Licensing** | Databricks runtime license | Open Source library | Commercial proprietary license | **100% Free & Open Source (Apache 2.0)** |

---

## Diagnostic Rules

All diagnostic rules are managed in `dlt_doctor.rules` and queryable via `dlt-doctor rules` and `dlt-doctor explain <RULE_ID>`.

> [!NOTE]
> Rules evaluated by `dlt-doctor analyze`: `DD001`, `DD002`, `DD003`, `DD004`, `PYSPARK-001`, `PYSPARK-002`, `PYSPARK-004`, `PYSPARK-005`, `PYSPARK-006`, and `SYNTAX-001`.
> `PLAN-001` through `PLAN-004` are evaluated by `PlanDoctor.analyze_explain_plan()` on query explain outputs (not run by CLI `analyze`).
> `UC-001` through `UC-004` are evaluated by `dlt-doctor uc-migrate` (not run by CLI `analyze`).

| Rule ID | Evaluated By | Severity | Name | Description |
| :--- | :--- | :--- | :--- | :--- |
| `DD001` | `analyze` | `MEDIUM` | Silent data loss from expect_or_drop policy | `expect_or_drop` silently drops non-compliant records from target tables. Dropped rows' contents are not persisted anywhere in the pipeline (only aggregate counts are logged). Use dual-stream quarantine routing to preserve invalid records. |
| `DD002` | `analyze` | `HIGH` | Quarantine filter is not NULL-safe complement | Quarantine filter does not handle SQL three-valued logic (NULL) for referenced columns. When a condition evaluates to NULL, both target and quarantine filters discard the row. |
| `DD003` | `analyze` | `INFO` | Dynamic expectation dictionary cannot be statically verified | Expectations are defined dynamically via runtime variables or external sources. Static analysis cannot resolve the expectation conditions. |
| `DD004` | `analyze` | `HIGH` | Non-deterministic function in expectation condition | Expectation condition uses non-deterministic functions (such as current_timestamp, rand, uuid). Non-deterministic expectations produce inconsistent validation results across streaming micro-batches. |
| `PYSPARK-001` | `analyze` | `CRITICAL` | Python UDF serialization overhead | Row-by-row JVM-to-Python socket serialization disables Catalyst vectorization. |
| `PYSPARK-002` | `analyze` | `HIGH` | Deep withColumn cascade | Chained `.withColumn()` calls create deep Project AST chains leading to compiler degradation. |
| `PYSPARK-004` | `analyze` | `MEDIUM` | Repartition triggers full shuffle | Explicit `.repartition()` triggers a full round-robin shuffle across workers. |
| `PYSPARK-005` | `analyze` | `HIGH` | Multiple action calls without DataFrame caching | Multiple actions on unpersisted DataFrame recompute upstream lineage from source. |
| `PYSPARK-006` | `analyze` | `CRITICAL` | Explicit Cartesian crossJoin | `crossJoin` produces M * N record multiplication, risking executor OOM and spill. |
| `SYNTAX-001` | `analyze` | `CRITICAL` | Source syntax error | Source file fails language parser validation. |
| `PLAN-001` | `explain_plan` | `CRITICAL` | Catalyst CartesianProduct operator in execution plan | Physical plan contains CartesianProduct operator without join condition. |
| `PLAN-002` | `explain_plan` | `MEDIUM` | SortMergeJoin utilized without Broadcast hints | SortMergeJoin executed where BroadcastExchange was feasible on smaller tables. |
| `PLAN-003` | `explain_plan` | `HIGH` | BatchEvalPython operator detected in physical plan | Catalyst is shipping rows across socket to Python worker daemon. |
| `PLAN-004` | `explain_plan` | `HIGH` | Empty PartitionFilters on partitioned table scan | Query scans partitioned table without partition pruning predicates. |
| `UC-001` | `uc-migrate` | `HIGH` | Legacy DBFS root mount path detected | Code references deprecated `/mnt/` or `dbfs:/` paths not secured by Unity Catalog. |
| `UC-002` | `uc-migrate` | `MEDIUM` | Legacy two-tier Hive Metastore table reference | Table references lack 3-level catalog namespace (`catalog.schema.table`). |
| `UC-003` | `uc-migrate` | `HIGH` | Hardcoded workspace local file path | Direct local filesystem references (`/tmp`, `/local`) will fail across distributed workers. |
| `UC-004` | `uc-migrate` | `HIGH` | Legacy DBFS mount API call detected | `dbutils.fs.mount()` is deprecated in Unity Catalog workspaces. |

---

## Terminal Scorecard Preview

<div align="center">

<img src="https://raw.githubusercontent.com/MsMansiDhruv/dlt-doctor/main/assets/dlt-doctor-terminal.svg" alt="dlt-doctor terminal CLI audit screenshot" width="100%" />

</div>

---

## Quickstart

### Interactive Walkthrough Notebook

Import [`notebooks/01_dlt_doctor_walkthrough.ipynb`](https://github.com/MsMansiDhruv/dlt-doctor/blob/main/notebooks/01_dlt_doctor_walkthrough.ipynb) directly into your Databricks workspace to run the interactive cell magic and AST optimizers live on your cluster.

### 1. Installation

```bash
pip install dlt-doctor
```

### 2. Pre-Commit & GitHub Actions CI Gate

Catch PySpark anti-patterns in pull requests before they merge:

```yaml
# .github/workflows/pyspark-audit.yml
name: PySpark Quality Gate
on: [pull_request]

jobs:
  audit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run dlt-doctor Quality Audit
        uses: MsMansiDhruv/dlt-doctor@v0.5.0
        with:
          path: "src/pipelines"
          format: "text"
```

### 3. Essential CLI Commands

```bash
# Statically audit a pipeline script, SQL file, notebook (.ipynb), or directory
dlt-doctor analyze src/pipelines/orders_silver.py
dlt-doctor analyze notebooks/01_ingest.ipynb
dlt-doctor analyze src/pipelines/orders.sql
dlt-doctor analyze src/pipelines --format markdown

# Auto-heal expect_or_drop anti-patterns into single-pass quarantine streams (preview diff)
dlt-doctor heal src/pipelines/orders_silver.py

# Apply healed quarantine routing directly to file or directory
dlt-doctor heal src/pipelines/orders_silver.py --write
dlt-doctor heal src/pipelines/ --write

# CI Compliance Gate: Check files or entire directory for unhealed drop rules
# Exit codes: 0 = Compliant, 1 = Needs healing, 2 = Usage/path error, 3 = Cannot heal
dlt-doctor heal src/pipelines/ --check

# List and explain diagnostic rules
dlt-doctor rules
dlt-doctor explain DD001

# Calculate late-arriving partition blast radius and emit pruned MERGE SQL
dlt-doctor restate --table main.gold.orders --late-date 2026-10-25 --window 7 --source main.silver.orders_recalculated

# Generate streaming CDC deduplication and SCD merge templates
dlt-doctor cdc --table main.silver.customers --keys customer_id --event-time updated_at

# Generate idempotent replay SQL to reprocess quarantined records
dlt-doctor replay --quarantine-table main.silver.orders_quarantine --target-table main.silver.orders --keys order_id --valid-filter "amount > 0" --erasure-table main.silver.orders_tombstones

# Audit legacy DBFS mounts and two-tier Hive Metastore references for Unity Catalog
dlt-doctor uc-migrate src/pipelines/orders.py --catalog prod_lakehouse --schema bronze

# Forecast Databricks DBU burn and cluster sizing (Serverless vs Classic DLT)
dlt-doctor cost --files-per-hour 25000 --avg-file-mb 12 --joins 2

# Inspect live Delta Lake table telemetry in Databricks (compaction debt, commit velocity, vacuum safety)
dlt-doctor inspect-table main.gold.orders --warehouse-id <warehouse_id>
```

### 4. Interactive Databricks Notebook Cell Magic

Audit PySpark code interactively inside Databricks or Jupyter notebook cells before cluster execution:

```python
# Load the extension in notebook setup cell
%load_ext dlt_doctor

%%dlt_doctor
# Audits cell Catalyst AST and flags anti-patterns before cluster execution
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

@udf
def parse_status(x):
    return x.lower() if x else "unknown"

df_orders = (
    spark.readStream.table("bronze_orders")
    .withColumn("c1", F.col("id"))
    .withColumn("c2", F.col("ts"))
    .withColumn("c3", F.col("amt"))
    .withColumn("c4", F.col("code"))
    .withColumn("status_clean", parse_status(F.col("status")))
)
```

---

## Production Walkthrough

### Act 1: Pre-Flight Code & Ingestion Health

Catch Catalyst compilation degradation, Auto Loader backpressure, and legacy storage paths before code touches an active cluster:

```python
from dlt_doctor import PlanDoctor, AdvisorDoctor, AutoLoaderDoctor

# 1. Statically audit PySpark code for chained withColumn calls and row-level UDFs
doctor = PlanDoctor()
findings = doctor.analyze_source_code("""
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

@udf
def clean_it(x):
    return x.strip() if x else None

df_clean = (
    df.withColumn("c1", F.col("a"))
      .withColumn("c2", F.col("b"))
      .withColumn("c3", F.col("c"))
      .withColumn("c4", F.col("d"))
      .withColumn("clean_val", clean_it(F.col("raw")))
      .repartition(4)
)
""")

report = AdvisorDoctor().compile_report("orders_etl", findings)
# Render scorecard directly inside a Databricks notebook cell or export HTML
scorecard_html = AdvisorDoctor().to_databricks_html(report)
# displayHTML(scorecard_html)

# 2. Audit Auto Loader ingestion stream progress for backpressure
al_doctor = AutoLoaderDoctor()
al_health = al_doctor.diagnose_stream_progress(
    {
        "numInputRows": 50000,
        "inputRowsPerSecond": 1200.0,
        "processedRowsPerSecond": 450.0,  # Falling behind ingestion rate
    }
)
if al_health.is_backpressured:
    print(f"Stream backpressured: Backlog ~{al_health.estimated_backlog_rows:,} rows.")
```

### Act 2: Streaming Execution & Zero-Loss Quality

Replace destructive `expect_or_drop` and prevent streaming CDC multiple-match merge failures:

```python
from dlt_doctor import ExpectationDoctor, ExpectationRule, CdcDoctor

# 1. Single-Pass NULL-Safe Quarantine (Valid -> Clean, Invalid -> Quarantine Table)
exp_doctor = ExpectationDoctor()
rules = [
    ExpectationRule("valid_id", "order_id IS NOT NULL"),
    ExpectationRule("positive_amount", "amount > 0"),
]
plan = exp_doctor.build_quarantine_routing("silver_orders", rules)

# Synthesizes complete single-pass pipeline code with failed rules tracking
pipeline_code = exp_doctor.generate_pipeline_code(plan, source_table="bronze_orders", flavor="dp")
print(pipeline_code)


# 2. Streaming CDC Micro-Batch Window Deduplication
cdc = CdcDoctor().generate_cdc_templates(
    target_table="main.silver.customers",
    primary_keys=["customer_id"],
    event_time_col="updated_at",
)
# Inside foreachBatch handler: dedupes micro-batch to eliminate Delta multiple-match exception
print(cdc.microbatch_dedup_code)
print(cdc.scd1_merge_sql)
```

### Act 3: Lakehouse Storage & Surgical Healing

Restate historical rolling metrics with surgical partition pruning and optimize Delta table storage:

```python
from dlt_doctor import RestatementDoctor, ClusterDoctor, ColumnProfile, VacuumDoctor

# 1. Surgical Late Data Restatement (Bounded Blast Radius)
reconciler = RestatementDoctor()
scope = reconciler.calculate_blast_radius(
    target_table="main.gold.fact_orders",
    late_date_iso="2026-10-25",
    window_size_days=7,
    source_table="main.silver.orders_recalculated",
    partition_col="order_date",
    match_key="order_id",
)
# Restates only the 7 affected partition days, skipping 358 unaffected days
spark.sql(scope.delta_merge_sql)

# 2. Select Optimal Liquid Clustering (CLUSTER BY) Keys
cluster_rec = ClusterDoctor().advise_clustering(
    table_name="main.gold.fact_orders",
    columns=[
        ColumnProfile(
            "customer_id", "string", distinct_count=25000, filter_frequency_score=0.9
        ),
        ColumnProfile(
            "order_date", "date", distinct_count=1800, filter_frequency_score=0.95
        ),
        ColumnProfile(
            "order_uuid", "string", distinct_count=9900000
        ),  # Rejected: near-unique
    ],
    total_rows=10000000,
)
spark.sql(cluster_rec.alter_cluster_ddl)

# 3. Safe Delta VACUUM Retention Guard
vac_report = VacuumDoctor().evaluate_vacuum_safety(
    table_name="main.silver.orders",
    requested_retention_hours=24,
    active_streaming_readers=2,
    cdf_enabled=True,
)
if vac_report.safety_level.value != "DANGEROUS":
    spark.sql(vac_report.recommended_vacuum_sql)
```

---

## Verification & Test Suite

The test suite validates all diagnostic engines and integration components across Python 3.10, 3.11, 3.12, and 3.13:

```bash
# Fast unit test suite (zero external cluster dependencies)
pytest -m "not integration"
# 197 passed in 0.52s

# Integration test suite (executed on local PySpark + Delta Lake)
REQUIRE_SPARK=1 pytest -m integration -rs -v
# 12 passed (executed Delta MERGE, SCD1/SCD2 CDC microbatches, and expectation pipelines)
```

All source code is audited with `ruff check src tests`, type-checked with `mypy src`, and verified against security vulnerabilities with `bandit -r src`.

For environment setup (including Windows `JAVA_HOME` and Hadoop binaries) and contributor guidelines, see [CONTRIBUTING.md](https://github.com/MsMansiDhruv/dlt-doctor/blob/main/CONTRIBUTING.md).

---

## Limitations & Operational Boundaries

- **Static AST Analysis**: Static analysis inspects source AST without cluster execution. Dynamic constructs (e.g. `eval()`, runtime-generated expectation dicts, or dynamic SQL queries) cannot be resolved statically (emits `DD003` INFO).
- **SQL Pipeline Auto-Heal**: `dlt-doctor heal` synthesizes Python AST definitions. SQL pipelines using `ON VIOLATION DROP ROW` must be manually migrated to dual-table quarantine structures.
- **Derived Non-Deterministic Rules**: Expectation conditions referencing runtime boundary calls (such as `current_date()`) emit a warning (`UserWarning`) because records evaluated near midnight boundaries may evaluate differently upon replay.
- **Configuration**: Audits and CI gates can be customized using `pyproject.toml` or `dlt-doctor.toml`. For complete options, see the [Configuration Guide](https://msmansidhruv.github.io/dlt-doctor/configuration/) and [Limitations Guide](https://msmansidhruv.github.io/dlt-doctor/limitations/).

---

## Disclaimers

`dlt-doctor` is an independent open-source project and is not affiliated with, sponsored by, or endorsed by Databricks, Inc. Apache Spark, Delta Lake, and Databricks are trademarks of their respective owners. Use of `dlt-doctor` does not guarantee regulatory compliance (such as GDPR, CCPA, or HIPAA); data engineering teams remain responsible for their own retention, masking, and governance policies.

---

## License

Distributed under the Apache 2.0 License. See [LICENSE](https://github.com/MsMansiDhruv/dlt-doctor/blob/main/LICENSE) for details.

<div align="center">
  <sub>Built by <a href="https://github.com/MsMansiDhruv">Mansi Dhruv</a> • Production Databricks &amp; Delta Lake Engineering</sub>
</div>
