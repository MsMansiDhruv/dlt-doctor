# dlt-doctor

<div align="center">
  <img src="assets/dlt-doctor-mascot.svg" alt="dlt-doctor visual identity" width="100%" />
</div>

**Production Optimization Engine and Quality Accelerator for Databricks DLT, Auto Loader, and PySpark**

[![PyPI](https://img.shields.io/badge/PyPI-dlt--doctor-3776AB?style=flat-square&logo=pypi&logoColor=white)](https://pypi.org/project/dlt-doctor/)
[![Runtime](https://img.shields.io/badge/Runtime-Databricks%20%7C%20Spark%203.5+-FF9B4A?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Storage](https://img.shields.io/badge/Storage-Delta%20Lake%20%26%20CDF-2EE6A7?style=flat-square&logo=delta&logoColor=white)](https://delta.io/)
[![Governance](https://img.shields.io/badge/Governance-Unity%20Catalog-0E1726?style=flat-square&logo=databricks&logoColor=2EE6A7)](https://www.databricks.com/product/unity-catalog)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue?style=flat-square)](https://github.com/MsMansiDhruv/dlt-doctor/blob/main/LICENSE)

---

## 15-Second Showcase

<div align="center">
  <img src="assets/dlt-doctor-showcase.gif" alt="dlt-doctor showcase preview" width="100%" />
</div>

---

## What is dlt-doctor?

`dlt-doctor` is an open-source static optimization engine and quality accelerator designed for Databricks Lakehouse pipelines. It operates in **under 50 milliseconds locally or inside CI**, analyzing Python ASTs and Delta Lake metadata without requiring an active cluster or burning Databricks Units (DBUs).

### Key Architectural Superpowers

- **Zero Cluster Spend**: Catches chained `.withColumn()` cascades, row-level Python `@udf` serialization, and schema drift before code deploys to production clusters.
- **Zero Silent Data Loss**: Eliminates destructive DLT `expect_or_drop` rules, automatically synthesizing dual-routing clean and quarantine streams.
- **Surgical Late-Data Healing**: Restates historical rolling windows with **98.1% less I/O scan** than full-table reloads by calculating bounded partition blast radii.
- **Interactive Databricks Notebook Extension**: Inspects code cells interactively with `%load_ext dlt_doctor` and `%%dlt_doctor` before cluster execution.
- **Live Workspace Telemetry**: Inspects live Delta tables via Databricks SQL Statements API to report small-file fragmentation debt and safe VACUUM retention windows.

---

## Quick Installation

```bash
pip install --upgrade dlt-doctor
```

## Quick Audit

```bash
# Statically audit an entire Lakehouse repository
dlt-doctor analyze src/pipelines --recursive

# Statically audit a Databricks notebook
dlt-doctor analyze notebooks/01_ingest.ipynb
```
