# Why DLT `expect_or_drop` Silently Purges Regulatory Data

### And How We Built a Zero-Loss Quarantine Stream & Catalyst AST Optimizer

*Author: Mansi Dhruv*  
*Published: November 2026*  
*Project: [dlt-doctor](https://github.com/MsMansiDhruv/dlt-doctor)*

---

## 1. The Hidden Cost of `expect_or_drop`

In modern lakehouse engineering, data quality constraints are often treated as simple filter expressions. In Databricks Delta Live Tables (DLT), the framework provides three native expectation policies:

1. `@dlt.expect("rule", "condition")`: Records the violation metric in the Delta event log, but permits the invalid row to pass downstream into curated Silver and Gold tables.
2. `@dlt.expect_or_fail("rule", "condition")`: Immediately halts the entire streaming pipeline when a single record violates the condition, blocking ingestion for all valid records until upstream producers fix the source data.
3. `@dlt.expect_or_drop("rule", "condition")`: Silently drops invalid records from the target table while recording an aggregate violation count in the pipeline telemetry log.

In non-critical web analytics, dropping corrupted telemetry records is often considered acceptable. However, in **regulated enterprise domains**—such as financial transaction processing (FINRA, SEC, Basel III), healthcare informatics (HIPAA), or supply-chain logistics—`expect_or_drop` introduces a dangerous structural vulnerability:

> **Data Dropped at Ingestion Is Data Permanently Lost.**  
> While the Delta event log counts how many rows were dropped, the actual payload, transaction ID, customer reference, and raw JSON data are discarded from the target Delta table.

When audit teams or downstream reconciliation jobs discover missing financial entries weeks later, engineers face an impossible task: the data was never written to any quarantine table, meaning it cannot be audited, inspected, corrected, or replayed without scanning raw cloud object storage archives.

---

## 2. Architectural Antipattern vs. Zero-Loss Quarantine

Consider a standard financial transaction ingestion pipeline written in DLT:

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(
    comment="Curated silver transactions"
)
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_account", "account_id IS NOT NULL")
def silver_transactions():
    return dlt.read_stream("bronze_transactions")
```

Under this implementation, any transaction with an invalid, zero, or null amount is dropped immediately. Downstream ledger reconciliations will balance against the Silver table, but the true transaction volume has silently diverged from the upstream payment provider.

### The Zero-Loss Dual-Stream Solution

To ensure zero data loss while preventing corrupt data from polluting Silver tables, production lakehouses require **dual-stream quarantine routing**:

```python
import dlt
from pyspark.sql.functions import col, current_timestamp, lit

@dlt.table(
    name="silver_transactions",
    comment="Validated transactions passing all regulatory checks"
)
def silver_transactions():
    return (
        dlt.read_stream("bronze_transactions")
        .filter((col("amount") > 0) & (col("account_id").isNotNull()))
    )

@dlt.table(
    name="silver_transactions_quarantine",
    comment="Quarantined records failing validation rules with reason metadata"
)
def silver_transactions_quarantine():
    return (
        dlt.read_stream("bronze_transactions")
        .filter(~((col("amount") > 0) & (col("account_id").isNotNull())))
        .withColumn("quarantine_timestamp", current_timestamp())
        .withColumn("violation_reason", lit("FAILED_AMOUNT_OR_ACCOUNT_VALIDATION"))
    )
```

With this architecture:
- Downstream business analytics consume clean, compliant records from `silver_transactions`.
- Compliance and operations teams monitor `silver_transactions_quarantine`.
- Every dropped record is retained with full payload attributes, allowing root-cause analysis, upstream escalation, and automated reprocessing.

---

## 3. Why Static AST Analysis Outperforms Runtime Discovery

Why do engineering teams continue to deploy `expect_or_drop` if the architectural risks are well understood?

The answer lies in **feedback loop latency**:
1. Pipeline authors test scripts against small synthetic sample datasets where all rows pass.
2. Code passes unit tests and is merged into `main`.
3. The pipeline runs in production on Databricks clusters consuming hundreds of DBUs per hour.
4. Only when edge cases appear in production does `expect_or_drop` begin silently purging records.

Discovering architectural flaws at runtime is expensive:
- Cluster startup overheads and serverless compute charges are incurred before syntax or configuration bugs are detected.
- Delta event log queries require custom SQL parsing to determine which rules are failing.

To solve this, we built **`dlt-doctor`**, an open-source static code analysis engine that audits PySpark and DLT source code at the Python Abstract Syntax Tree (AST) level—before any JVM or cluster is initialized.

---

## 4. Inside the `dlt-doctor` Static Optimizer

`dlt-doctor` parses pipeline code into AST nodes using Python's native `ast` module, avoiding heavy JVM dependencies or Databricks Connect requirements. It runs in under **0.05 seconds** in CI/CD pipelines or local pre-commit hooks.

### Intercepting Expectation Vulnerabilities

The expectation diagnostic engine (`ExpectationDoctor`) traverses AST decorator nodes:

```python
class ExpectationDoctor:
    def audit_expectations(self, tree: ast.AST) -> List[AuditFinding]:
        findings = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                for decorator in node.decorator_list:
                    if self._is_expect_or_drop(decorator):
                        findings.append(AuditFinding(
                            severity="HIGH",
                            engine="ExpectationDoctor",
                            message=f"Function '{node.name}' uses expect_or_drop. Risk of unrecoverable data loss in regulated pipelines.",
                            suggestion="Refactor into dual-stream routing with an explicit quarantine table."
                        ))
        return findings
```

When run against pipeline code, `dlt-doctor` immediately flags the pattern:

```bash
$ dlt-doctor analyze pipelines/transactions.py

[HIGH] ExpectationDoctor: Function 'silver_transactions' uses expect_or_drop.
       Risk of unrecoverable data loss in regulated pipelines.
       Rule: 'valid_amount' -> ('amount > 0')
       Recommendation: Run 'dlt-doctor auto-heal pipelines/transactions.py' to generate dual-stream quarantine.
```

### Automated Code Generation (Auto-Heal)

Running `--auto-heal` inspects the AST expression and synthesizes the corresponding quarantine table definition automatically, preserving original source formatting while injecting the companion table.

---

## 5. Beyond Expectations: 12 Static Engine Checks

While zero-loss expectations address regulatory data integrity, distributed performance bottlenecks remain equally critical. `dlt-doctor` bundles 12 specialized diagnostic engines:

1. **Catalyst Plan Bloat**: Intercepts unpruned chained transformations and redundant project/filter operations that exhaust JVM Catalyst query optimizer memory.
2. **Surgical Partition Restatements**: Detects unbounded Delta `MERGE` statements lacking target partition predicates (`ON t.id = s.id`), generating bounded date-window clauses (`AND t.txn_date >= CURRENT_DATE - INTERVAL 7 DAYS`) that prevent full-table scans.
3. **Liquid Clustering Advisory**: Replaces legacy multi-column partitioning schemes with Databricks `CLUSTER BY` specifications, eliminating file skew and metadata footprint overhead.
4. **Broadcast Join Guardrails**: Intercepts `broadcast(large_df)` invocations exceeding safe JVM limits (10MB default) that risk driver Out-Of-Memory (OOM) crashes.
5. **Unity Catalog Migration**: Maps legacy Hive Metastore `/mnt/` mount points and two-part table namespaces (`schema.table`) to three-level catalogs (`catalog.schema.table`).
6. **Compaction Debt Detector**: Analyzes file size distributions to identify small-file fragmentation before write amplification cascades into query latency.
7. **Workspace Telemetry Connector**: Connects to Databricks SQL Warehouses via the official Databricks SDK to audit live Delta table histories and commit velocities.

---

## 6. Integrating into Developer Workflows

### 1. In Local CI/CD (GitHub Action)

Add `dlt-doctor` to your PR validation workflow to reject data loss anti-patterns before merge:

```yaml
- name: Audit Lakehouse Pipelines
  uses: MsMansiDhruv/dlt-doctor@v0.4.2
  with:
    path: 'notebooks/'
    fail-on: 'HIGH'
```

### 2. In Interactive Databricks Notebooks

Install `dlt-doctor` directly into your interactive cluster:

```python
%pip install dlt-doctor
%load_ext dlt_doctor.magic
```

Then audit any pipeline definition cell in place:

```python
%%dlt_doctor
@dlt.table
@dlt.expect_or_drop("valid_price", "price > 0")
def clean_orders():
    return dlt.read_stream("raw_orders")
```

The cell immediately renders a visual diagnostic report with health scores, severity badges, and refactoring recommendations.

---

## 7. Conclusion

Data quality policies must not sacrifice regulatory auditability. By moving quality validation from reactive runtime monitoring to proactive static analysis:
- Pipeline bugs are caught at code-review time in milliseconds.
- High-severity data loss risks like `expect_or_drop` are systematically replaced with zero-loss quarantine architecture.
- Cloud compute costs are preserved for actual workload execution rather than failing jobs.

`dlt-doctor` is open source, licensed under Apache 2.0, and available on PyPI.

- **PyPI**: `pip install dlt-doctor`
- **Documentation**: [https://msmansidhruv.github.io/dlt-doctor/](https://msmansidhruv.github.io/dlt-doctor/)
- **GitHub**: [https://github.com/MsMansiDhruv/dlt-doctor](https://github.com/MsMansiDhruv/dlt-doctor)
