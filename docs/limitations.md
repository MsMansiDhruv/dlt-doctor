# Limitations and Boundaries

`dlt-doctor` is designed for fast, local, zero-cluster static analysis and deterministic SQL template synthesis. Understanding its operational boundaries ensures appropriate deployment across development workflows and CI pipelines.

## Static Analysis vs. Runtime Profiling

| Capability | Supported Statically | Requires Active Cluster / SQL Warehouse |
| :--- | :--- | :--- |
| **Catalyst AST Inspection** | Yes (Python AST, SQL parsing) | No |
| **Anti-Pattern Detection** (`PYSPARK-001..006`) | Yes | No |
| **Quarantine Synthesis** (`heal`) | Yes | No |
| **Unity Catalog Migration Audit** (`UC-001..004`) | Yes | No |
| **Runtime Data Skew Measurement** | No | Yes (Spark execution metrics) |
| **Physical Plan Explain Inspection** | Yes (when explain output is supplied) | Yes (to run `df.explain()`) |
| **Live Delta Table Telemetry** | No | Yes (`dlt-doctor inspect-table`) |

## Dynamic Expectation Dictionaries (`DD003`)

Static AST visitors cannot evaluate runtime variables or external metadata lookups. For example:

```python
# Statically resolvable literal (Passed)
@dlt.expect_all_or_drop({"valid_id": "id IS NOT NULL", "valid_amt": "amount > 0"})
def orders(): ...

# Dynamic dictionary loaded from external JSON or database (Emits DD003)
rules_dict = load_rules_from_catalog("orders")
@dlt.expect_all_or_drop(rules_dict)
def orders(): ...
```

When an unresolvable dynamic expression is encountered, `dlt-doctor` emits `DD003` (INFO) and refuses `heal` to prevent invalid code generation.

## SQL Dialect Handling

`dlt-doctor` uses `sqlglot` targeting the `spark` dialect to validate expectation conditions and parse SQL statements. Complex Spark SQL constructs that rely on custom Catalyst expressions or unreleased Spark dialect extensions may not parse with standard tooling. In such cases, inline suppressions (`# dlt-doctor: ignore[...]`) should be applied.

## Network & Connectivity Requirements

- Unit tests, AST audits (`dlt-doctor analyze`), rule listings (`dlt-doctor rules`), and diff synthesizers (`dlt-doctor heal`) run completely offline with no network, JVM, or cluster requirements.
- Live inspection (`dlt-doctor inspect-table`) requires network access to the Databricks SQL Statements API and valid workspace credentials (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID`).
