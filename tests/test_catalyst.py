"""Tests for PlanDoctor and Catalyst AST diagnostics."""

from dlt_doctor.catalyst import DiagnosticSeverity, PlanDoctor


def test_detect_python_udf() -> None:
    code = """
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def clean_str(val):
    return val.strip().lower()

df = spark.table("raw_data").withColumn("clean_val", clean_str(F.col("raw_val")))
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code)

    assert any(f.rule_id == "PYSPARK-001" for f in findings)
    udf_finding = next(f for f in findings if f.rule_id == "PYSPARK-001")
    assert udf_finding.severity == DiagnosticSeverity.CRITICAL
    assert "serialization" in udf_finding.impact_summary.lower()


def test_detect_with_column_cascade() -> None:
    code = """
df_clean = (
    df
    .withColumn("c1", F.col("a"))
    .withColumn("c2", F.col("b"))
    .withColumn("c3", F.col("c"))
    .withColumn("c4", F.col("d"))
    .withColumn("c5", F.col("e"))
)
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code)

    assert any(f.rule_id == "PYSPARK-002" for f in findings)
    cascade_finding = next(f for f in findings if f.rule_id == "PYSPARK-002")
    assert cascade_finding.severity == DiagnosticSeverity.HIGH
    assert "df.withColumns" in cascade_finding.recommendation


def test_detect_repartition_before_write() -> None:
    code = """
df.repartition(4).write.format("delta").saveAsTable("main.gold.summary")
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code)

    assert any(f.rule_id == "PYSPARK-004" for f in findings)
    repart_finding = next(f for f in findings if f.rule_id == "PYSPARK-004")
    assert "coalesce(4)" in repart_finding.recommendation


def test_detect_cross_join() -> None:
    code = """
joined_df = df_a.crossJoin(df_b)
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code)

    assert any(f.rule_id == "PYSPARK-006" for f in findings)


def test_analyze_plan_string_cartesian_and_udf() -> None:
    plan_mock = """
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- CartesianProduct
   :- Scan parquet default.orders
   +- BatchEvalPython [clean_str(name#12)]
      +- Scan parquet default.customers
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_plan_string(plan_mock)

    assert any(f.rule_id == "PLAN-001" for f in findings)
    assert any(f.rule_id == "PLAN-003" for f in findings)
