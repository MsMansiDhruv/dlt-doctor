"""Reproduce tests for bugs and missing claims identified in code review."""

import html
import tempfile
from pathlib import Path

import pytest

from dlt_doctor.advisor import AdvisorDoctor, PipelineAuditReport
from dlt_doctor.catalyst import DiagnosticFinding, DiagnosticSeverity, PlanDoctor
from dlt_doctor.cdc import CdcDoctor
from dlt_doctor.expectations import ExpectationDoctor, ExpectationRule
from dlt_doctor.reconciler import RestatementDoctor
from dlt_doctor.scanner import DirectoryScanner
from dlt_doctor.uc import UcDoctor


def test_reproduce_dd001_expect_or_drop_not_intercepted():
    """Claim: 'Intercepts expect_or_drop anti-patterns'.
    Bug: analyze_source_code does not flag @dlt.expect_or_drop.
    """
    code = """
import dlt

@dlt.table(name="silver_orders")
@dlt.expect_or_drop("valid_amount", "amount > 0")
def silver_orders():
    return dlt.read_stream("bronze_orders")
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code)
    rule_ids = [f.rule_id for f in findings]
    assert "DD001" in rule_ids, f"Expected DD001 in findings, got {rule_ids}"


def test_reproduce_null_safe_quarantine_routing_gap():
    """Bug: invalid_filter is NOT (valid_filter).
    When amount is NULL, amount > 0 is NULL, NOT(NULL) is NULL in SQL 3VL.
    Row is excluded from both clean and quarantine streams.
    Proper routing must use coalesce(<cond>, false).
    """
    doctor = ExpectationDoctor()
    rules = [ExpectationRule(rule_name="valid_amount", condition_sql="amount > 0")]
    plan = doctor.build_quarantine_routing("silver_orders", rules)
    assert "coalesce((amount > 0), false)" in plan.valid_filter_sql
    assert (
        "NOT (coalesce((amount > 0), false))" in plan.invalid_filter_sql
        or "NOT (" in plan.invalid_filter_sql
    )


def test_reproduce_duplicate_udf_finding():
    """Bug: @udf(StringType()) is reported twice (in visit_FunctionDef and visit_Call)."""
    code = """
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

@udf(StringType())
def clean_text(s):
    return s.strip() if s else None
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code)
    udf_findings = [f for f in findings if f.rule_id == "PYSPARK-001"]
    assert len(udf_findings) == 1, (
        f"Expected exactly 1 PYSPARK-001 finding, got {len(udf_findings)}"
    )


def test_reproduce_withcolumn_cascade_duplicates():
    """Bug: A chain of 5 withColumn calls yields multiple findings because each sub-chain >= 4 matches."""
    code = """
df = (
    raw_df
    .withColumn("a", col("x") + 1)
    .withColumn("b", col("x") + 2)
    .withColumn("c", col("x") + 3)
    .withColumn("d", col("x") + 4)
    .withColumn("e", col("x") + 5)
)
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code)
    cascade_findings = [f for f in findings if f.rule_id == "PYSPARK-002"]
    assert len(cascade_findings) == 1, (
        f"Expected exactly 1 PYSPARK-002 finding for the chain, got {len(cascade_findings)}"
    )


def test_reproduce_notebook_sql_cell_syntax_crash():
    """Bug: Notebook extract_notebook_code only comments the %sql line,
    leaving SQL body to reach ast.parse and failing with SYNTAX-001.
    """
    import json

    notebook_content = {
        "cells": [
            {
                "cell_type": "code",
                "source": ["%sql\n", "SELECT * FROM bronze_orders WHERE amount > 0;\n"],
            },
            {"cell_type": "code", "source": ["df = spark.read.table('silver_orders')\n"]},
        ]
    }
    with tempfile.NamedTemporaryFile("w", suffix=".ipynb", delete=False, encoding="utf-8") as f:
        json.dump(notebook_content, f)
        temp_path = f.name

    try:
        scanner = DirectoryScanner()
        report = scanner.scan_directory(temp_path)
        syntax_findings = [f for _, f in report.all_findings if f.rule_id == "SYNTAX-001"]
        assert len(syntax_findings) == 0, (
            f"Expected no SYNTAX-001 crash on notebook with %sql, got: {syntax_findings}"
        )
    finally:
        Path(temp_path).unlink(missing_ok=True)


def test_reproduce_load_ipython_extension_export():
    """Bug: %load_ext dlt_doctor fails because load_ipython_extension is not exported from dlt_doctor root."""
    import dlt_doctor

    assert hasattr(dlt_doctor, "load_ipython_extension"), (
        "load_ipython_extension not exported from dlt_doctor"
    )


def test_reproduce_sql_file_scan_crash():
    """Bug: analyze on .sql file runs ast.parse and fails with SYNTAX-001."""
    # When analyzing a SQL file directly
    from dlt_doctor.cli import build_parser

    parser = build_parser()
    args = parser.parse_args(["analyze", "dummy.sql"])
    assert hasattr(args, "path")
    # PlanDoctor currently parses everything as Python AST:
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code("SELECT * FROM silver_orders;")
    # It must not report SYNTAX-001 on valid SQL when SQL is supported
    syntax_errors = [f for f in findings if f.rule_id == "SYNTAX-001"]
    assert len(syntax_errors) == 0, (
        f"SQL was parsed with Python ast and produced SYNTAX-001: {syntax_errors}"
    )


def test_reproduce_uc003_false_positive_on_dlt_read():
    """Bug: dlt.read_stream('bronze_orders') is flagged by UC-003 as a two-tier violation,
    even though single-name pipeline tables are idiomatic in DLT.
    """
    code = """
import dlt

@dlt.table
def silver_orders():
    return dlt.read_stream("bronze_orders")
"""
    uc = UcDoctor()
    findings = uc.audit_code(code)
    assert len(findings) == 0, (
        f"Expected no UC findings for idiomatic dlt.read_stream, got: {findings}"
    )


def test_reproduce_generated_quarantine_code_syntax():
    """Bug: generate_dlt_decorator_code creates invalid function name def main.silver.orders():
    and references undefined F.
    """
    doctor = ExpectationDoctor()
    rules = [ExpectationRule(rule_name="valid_amount", condition_sql="amount > 0")]
    plan = doctor.build_quarantine_routing("main.silver.orders", rules)
    code = doctor.generate_dlt_decorator_code(plan)
    import ast

    try:
        ast.parse(code)
    except SyntaxError as e:
        pytest.fail(f"Generated quarantine code has syntax error: {e}\nCode:\n{code}")


def test_reproduce_html_escape_in_advisor():
    """Bug: advisor.to_databricks_html interpolates unescaped code strings."""
    advisor = AdvisorDoctor()
    finding = DiagnosticFinding(
        rule_id="TEST-001",
        severity=DiagnosticSeverity.HIGH,
        title="Test <script>alert(1)</script>",
        impact_summary="Impact with <b>unescaped</b> html",
        recommendation="Fix with <i>tag</i>",
    )
    report = PipelineAuditReport(
        pipeline_name="<script>bad</script>",
        overall_health_score=80,
        findings=[finding],
        estimated_dbu_savings_pct=10.0,
        summary_text="summary",
    )
    rendered_html = advisor.to_databricks_html(report)
    assert "<script>" not in rendered_html
    assert html.escape("<script>alert(1)</script>") in rendered_html


def test_reproduce_cdc_scd2_placeholder_invalid_sql():
    """Bug: SCD2 merge SQL contains literal '...' placeholders that cannot parse as valid SQL."""
    import sqlglot

    cdc = CdcDoctor()
    tmpl = cdc.generate_cdc_templates("silver_customers", ["id"], "updated_at")
    assert tmpl.scd2_merge_sql != ""
    # Literal ... in SQL will cause sqlglot parse failure
    try:
        sqlglot.parse_one(tmpl.scd2_merge_sql, read="spark")
    except Exception as exc:
        pytest.fail(f"SCD2 SQL template failed to parse with sqlglot: {exc}")


def test_reproduce_restate_missing_delete_unmatched():
    """Bug: restate SQL lacks WHEN NOT MATCHED BY SOURCE ... THEN DELETE."""
    reconciler = RestatementDoctor()
    scope = reconciler.calculate_blast_radius(
        "silver_orders", "2026-11-01", 7, delete_unmatched=True
    )
    assert "WHEN NOT MATCHED BY SOURCE" in scope.delta_merge_sql, (
        f"Restatement SQL missing WHEN NOT MATCHED BY SOURCE: {scope.delta_merge_sql}"
    )


def test_reproduce_auto_heal_cli_flag():
    """Bug: analyze CLI has no --fix / heal command."""
    from dlt_doctor.cli import build_parser

    parser = build_parser()
    # Check if heal subcommand exists or analyze --fix exists
    subcommands = parser._subparsers._actions[1].choices
    assert "heal" in subcommands or "--fix" in parser.format_help()
