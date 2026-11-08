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
    scorecard_html = AdvisorDoctor().to_databricks_html(report)
    assert scorecard_html is not None
    assert len(scorecard_html) > 0
    assert len(findings) >= 3

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
    pipeline_code = exp_doctor.generate_pipeline_code(
        plan, source_table="bronze_orders", flavor="dp"
    )
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
        source_table="main.silver.orders_recalculated",
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
            ColumnProfile("order_date", "date", distinct_count=1800, filter_frequency_score=0.95),
            ColumnProfile("order_uuid", "string", distinct_count=9900000),
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
                    raise AssertionError(
                        f"Failed to parse README python block: {exc}\nCode:\n{clean_code}"
                    ) from exc
        elif in_python_block:
            current_block.append(line)


def test_readme_cli_commands_parse_cleanly(tmp_path: Path) -> None:
    """Extract all `dlt-doctor ...` commands from README.md and verify they parse and validate cleanly."""
    import shlex

    from dlt_doctor.cli import build_parser

    readme_path = Path(__file__).parent.parent / "README.md"
    content = readme_path.read_text(encoding="utf-8")
    parser = build_parser()

    # Extract all lines beginning with dlt-doctor
    cli_lines: list[str] = []
    for line in content.splitlines():
        line_clean = line.strip()
        if line_clean.startswith("dlt-doctor "):
            cli_lines.append(line_clean)

    assert len(cli_lines) >= 10, (
        f"Expected at least 10 CLI examples in README, found {len(cli_lines)}"
    )

    # Mock sample files for commands taking paths
    mock_py = tmp_path / "mock_pipe.py"
    mock_py.write_text("import dlt\n@dlt.table\ndef t(): return dlt.read('s')\n", encoding="utf-8")
    mock_sql = tmp_path / "mock_pipe.sql"
    mock_sql.write_text("SELECT 1;\n", encoding="utf-8")
    mock_nb = tmp_path / "mock_nb.ipynb"
    mock_nb.write_text('{"cells": []}', encoding="utf-8")
    mock_dir = tmp_path / "pipes"
    mock_dir.mkdir()

    for cmd in cli_lines:
        tokens = shlex.split(cmd)
        assert tokens[0] == "dlt-doctor"
        args_list = tokens[1:]

        # Normalize placeholders or demo paths to local temp mocks
        normalized_args: list[str] = []
        for token in args_list:
            if "<warehouse_id>" in token:
                normalized_args.append("mock_wh_12345")
            elif token.endswith(".py"):
                normalized_args.append(str(mock_py))
            elif token.endswith(".sql"):
                normalized_args.append(str(mock_sql))
            elif token.endswith(".ipynb"):
                normalized_args.append(str(mock_nb))
            elif token in {"src/pipelines", "src/pipelines/"}:
                normalized_args.append(str(mock_dir))
            else:
                normalized_args.append(token)

        # 1. Parsing must succeed with zero SystemExit/argparse errors
        parsed = parser.parse_args(normalized_args)
        assert parsed is not None, f"Failed to parse CLI command: {cmd}"

        # 2. For pure SQL generators and local utilities, verify execution exits 0
        from dlt_doctor.cli import main

        if parsed.command in {"rules", "explain", "restate", "cdc", "replay", "cost"}:
            exit_code = main(normalized_args)
            assert exit_code == 0, f"Command '{cmd}' returned non-zero exit code: {exit_code}"
