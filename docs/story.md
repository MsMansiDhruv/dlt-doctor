# The Story

Every data engineer working on Databricks knows the exact feeling:

It is 2 AM. Your streaming pipeline crashes with:
```text
DeltaAnalysisException: Cannot perform Merge as multiple source rows matched
```

Or worse: native Delta Live Tables `expect_or_drop` silently purged 50,000 regulatory transactions from the dataset, and nobody noticed until compliance audit season six months later.

You spin up an interactive compute cluster to debug, burning expensive DBUs while waiting 4 minutes for workers to allocate, only to discover that someone chained 8 `.withColumn()` calls and wrote a row-level Python `@udf` that disabled Catalyst Whole-Stage CodeGen and created massive JVM-to-Python socket serialization.

Meanwhile, enterprise SaaS observability platforms (Monte Carlo, Anomalo, Unravel Data, Acceldata) cost \$50,000 to \$150,000 annually just to send a Slack notification *after* corrupted data has already entered downstream Gold tables and analytical marts.

---

## The Antidote

`dlt-doctor` shifts quality and optimization left:

- **Pre-Flight Static Auditing**: Parses the Python Catalyst AST in under 50 milliseconds locally or inside GitHub Actions before pull requests merge.
- **Dual-Routing Quarantine**: Routes compliant rows to Silver and quarantined records to a dead-letter Delta table with automated violation metadata.
- **Surgical Restatement**: Restates historical late-arriving partitions with 98.1% less I/O scan than full-table reloads.
- **Free and Open Source**: Apache 2.0 licensed, single `pip install` footprint, zero telemetry daemons, zero external VPC peering.
