# Catalyst AST Optimizer (`PlanDoctor`)

`PlanDoctor` statically analyzes the Abstract Syntax Tree (AST) of PySpark code without needing an active Spark session.

## Rules Audited

1. **Chained `.withColumn()` Cascades (`PYSPARK-002`)**:
   - *Problem*: Each call appends a separate `Project` operator to the Catalyst logical plan, degrading plan compilation and risking JVM `StackOverflowError`.
   - *Fix*: Synthesizes `df.withColumns({...})` (Spark 3.3+) or single `df.select('*', ...)`.
2. **Row-Level Python `@udf` Execution (`PYSPARK-001`)**:
   - *Problem*: Disables Catalyst Whole-Stage CodeGen and creates heavy JVM-to-Python socket serialization.
   - *Fix*: Emits vectorized Arrow `@pandas_udf` or native `F.when(...)` expressions.
3. **Premature Repartitioning (`PYSPARK-003`)**:
   - *Problem*: Explicit `repartition()` before filters defeats Spark 3.0+ Adaptive Query Execution (AQE).
   - *Fix*: Advises removing manual shuffle barriers.

## Python API Usage

```python
from dlt_doctor import PlanDoctor, AdvisorDoctor

doctor = PlanDoctor()
findings = doctor.analyze_source_code("""
df_clean = (
    df.withColumn("c1", F.col("a"))
      .withColumn("c2", F.col("b"))
      .withColumn("c3", my_udf(F.col("c")))
)
""")

report = AdvisorDoctor().compile_report("orders_staging", findings)
print(f"Health Score: {report.overall_health_score}/100")
print(f"Est. Savings: ~{report.estimated_dbu_savings_pct}%")
```
