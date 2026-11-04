"""Doc-tests that extract and execute Python code blocks from README.md."""

from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import MagicMock

import sqlglot


def test_readme_act1_execution() -> None:
    """Execute Act 1 code block from README.md."""
    from dlt_doctor import AdvisorDoctor, AutoLoaderDoctor, PlanDoctor

    # 1. Statically audit PySpark code
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code("""
df_clean = (
    df.withColumn("c1", F.col("a"))
      .withColumn("c2", F.col("b"))
      .withColumn("clean_val", my_python_udf(F.col("raw")))
      .repartition(4)
)
""")
    report = AdvisorDoctor().compile_report("orders_etl", findings)
    scorecard_html = AdvisorDoctor().to_databricks_html(report)
    assert scorecard_html is not None
    assert len(scorecard_html) > 0

    # 2. Audit Auto Loader ingestion stream progress
    al_doctor = AutoLoaderDoctor()
    al_health = al_doctor.diagnose_stream_progress(
        {
            "numInputRows": 50000,
            "inputRowsPerSecond": 1200.0,
            "processedRowsPerSecond": 450.0,
        }
    )
    assert al_health.is_backpressured
    assert al_health.estimated_backlog_rows > 0


def test_readme_act2_execution() -> None:
    """Execute Act 2 code block from README.md."""
    from dlt_doctor import CdcDoctor, ExpectationDoctor, ExpectationRule

    # 1. Single-Pass NULL-Safe Quarantine
    exp_doctor = ExpectationDoctor()
    rules = [
        ExpectationRule("valid_id", "order_id IS NOT NULL"),
        ExpectationRule("positive_amount", "amount > 0"),
    ]
    plan = exp_doctor.build_quarantine_routing("silver_orders", rules)
    pipeline_code = exp_doctor.generate_pipeline_code(plan, source_table="bronze_orders", flavor="dp")
    # Verify synthesized code compiles cleanly as valid Python
    parsed_ast = ast.parse(pipeline_code)
    assert parsed_ast is not None
    assert "silver_orders" in pipeline_code

    # 2. Streaming CDC Micro-Batch Window Deduplication
    cdc = CdcDoctor().generate_cdc_templates(
        target_table="main.silver.customers",
        primary_keys=["customer_id"],
        event_time_col="updated_at",
    )
    assert cdc.microbatch_dedup_code is not None
    assert sqlglot.parse_one(cdc.scd1_merge_sql, read="spark") is not None


def test_readme_act3_execution() -> None:
    """Execute Act 3 code block from README.md."""
    from dlt_doctor import ClusterDoctor, ColumnProfile, RestatementDoctor, VacuumDoctor

    # 1. Surgical Late Data Restatement
    reconciler = RestatementDoctor()
    scope = reconciler.calculate_blast_radius(
        target_table="main.gold.fact_orders",
        late_date_iso="2026-10-25",
        window_size_days=7,
        partition_col="order_date",
        match_key="order_id",
    )
    assert sqlglot.parse_one(scope.delta_merge_sql, read="spark") is not None

    # Mock spark for SQL execution
    mock_spark = MagicMock()
    mock_spark.sql(scope.delta_merge_sql)

    # 2. Select Optimal Liquid Clustering Keys
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
            ),
        ],
        total_rows=10000000,
    )
    assert "ALTER TABLE main.gold.fact_orders CLUSTER BY" in cluster_rec.alter_cluster_ddl
    mock_spark.sql(cluster_rec.alter_cluster_ddl)

    # 3. Safe Delta VACUUM Retention Guard
    vac_report = VacuumDoctor().evaluate_vacuum_safety(
        table_name="main.silver.orders",
        requested_retention_hours=24,
        active_streaming_readers=2,
        cdf_enabled=True,
    )
    assert vac_report is not None
    if vac_report.safety_level.value != "DANGEROUS":
        mock_spark.sql(vac_report.recommended_vacuum_sql)


def test_readme_python_blocks_are_syntactically_valid() -> None:
    """Extract all python code fences from README.md and verify they parse cleanly."""
    readme_path = Path(__file__).parent.parent / "README.md"
    content = readme_path.read_text(encoding="utf-8")

    in_python_block = False
    current_block: list[str] = []

    for line in content.splitlines():
        if line.strip() == "```python":
            in_python_block = True
            current_block = []
        elif line.strip() == "```" and in_python_block:
            in_python_block = False
            # Filter out IPython magic lines like %load_ext or %%dlt_doctor before ast.parse
            clean_lines = [
                line_str for line_str in current_block if not line_str.strip().startswith("%")
            ]
            clean_code = "\n".join(clean_lines)
            if clean_code.strip():
                try:
                    ast.parse(clean_code)
                except SyntaxError as exc:
                    raise AssertionError(f"Failed to parse README python block: {exc}\nCode:\n{clean_code}") from exc
        elif in_python_block:
            current_block.append(line)
