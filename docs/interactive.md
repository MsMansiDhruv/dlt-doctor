# Interactive Databricks Notebooks

`dlt-doctor` includes native IPython / Databricks notebook integration so engineers can audit pipeline code cells interactively before executing jobs on clusters.

## Loading the Extension

In your initial notebook setup cell, run:
```python
%pip install --upgrade dlt-doctor
%load_ext dlt_doctor
```

---

## Using `%%dlt_doctor` Cell Magic

Prefix any pipeline definition cell with `%%dlt_doctor` followed by an optional cell title:

```python
%%dlt_doctor Silver Orders Staging View
from pyspark.sql import functions as F

# Anti-pattern: Chained withColumn calls causing deep Project node cascades
df_orders = (
    spark.readStream.table("bronze_orders")
    .withColumn("quantity_clean", F.expr("try_cast(quantity AS INT)"))
    .withColumn("unit_price_clean", F.expr("try_cast(unit_price AS DECIMAL(18,2))"))
    .withColumn("total_amount_clean", F.expr("try_cast(total_amount AS DECIMAL(18,2))"))
    .withColumn("order_timestamp_clean", F.expr("try_cast(order_timestamp AS TIMESTAMP)"))
)
```

### Native HTML Scorecard Rendering

When run inside a Databricks notebook, `dlt-doctor` detects the runtime environment and invokes `displayHTML()` directly, rendering a dark-mode scorecard detailing:
1. **Pipeline Health Score** (e.g. 70/100)
2. **Estimated DBU Savings** (e.g. ~30%)
3. **Specific Line-Level Fixes** and rewritten PySpark code snippets

---

## Runnable Demo Notebook

A ready-to-run Databricks notebook is included in the repository at:
[`notebooks/01_dlt_doctor_walkthrough.ipynb`](https://github.com/MsMansiDhruv/dlt-doctor/blob/main/notebooks/01_dlt_doctor_walkthrough.ipynb)
