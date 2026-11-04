"""Tests for scanner quality, notebook per-cell parsing, SQL support, and formats (WP5)."""

import json
import tempfile
from pathlib import Path

from dlt_doctor.advisor import AdvisorDoctor
from dlt_doctor.catalyst import DiagnosticSeverity, PlanDoctor
from dlt_doctor.config import DoctorConfig
from dlt_doctor.scanner import DirectoryScanner


def test_notebook_parsed_cell_by_cell_without_crash() -> None:
    notebook_content = {
        "cells": [
            {
                "cell_type": "code",
                "source": ["%md\n", "# Markdown title\n"]
            },
            {
                "cell_type": "code",
                "source": ["%sql\n", "SELECT * FROM bronze_orders;\n"]
            },
            {
                "cell_type": "code",
                "source": [
                    "import dlt\n",
                    "@dlt.table\n",
                    "def silver():\n",
                    "    return dlt.read_stream('bronze')\n"
                ]
            }
        ]
    }
    with tempfile.NamedTemporaryFile("w", suffix=".ipynb", delete=False, encoding="utf-8") as f:
        json.dump(notebook_content, f)
        temp_path = f.name

    try:
        scanner = DirectoryScanner()
        report = scanner.scan_directory(temp_path)
        syntax_errors = [f for _, f in report.all_findings if f.rule_id == "SYNTAX-001"]
        assert len(syntax_errors) == 0
        assert report.total_files_scanned == 1
    finally:
        Path(temp_path).unlink(missing_ok=True)


def test_databricks_py_notebook_format() -> None:
    content = """# Databricks notebook source
# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM bronze;

# COMMAND ----------
import dlt

@dlt.table
def clean_tbl():
    return dlt.read_stream("bronze")
"""
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False, encoding="utf-8") as f:
        f.write(content)
        temp_path = f.name

    try:
        scanner = DirectoryScanner()
        report = scanner.scan_directory(temp_path)
        syntax_errors = [f for _, f in report.all_findings if f.rule_id == "SYNTAX-001"]
        assert len(syntax_errors) == 0
    finally:
        Path(temp_path).unlink(missing_ok=True)


def test_sql_file_scanning() -> None:
    sql_text = """
    -- Table definition with expectation
    CREATE OR REFRESH STREAMING TABLE clean_txns (
        CONSTRAINT valid_amt EXPECT (amount > 0) ON VIOLATION DROP ROW
    ) AS SELECT * FROM STREAM(/mnt/raw/txns);
    """
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False, encoding="utf-8") as f:
        f.write(sql_text)
        temp_path = f.name

    try:
        scanner = DirectoryScanner()
        report = scanner.scan_directory(temp_path)
        rule_ids = {f.rule_id for _, f in report.all_findings}
        assert "DD001" in rule_ids
        assert "UC-001" in rule_ids
        assert "SYNTAX-001" not in rule_ids
    finally:
        Path(temp_path).unlink(missing_ok=True)


def test_suppressions_inline_and_file_level() -> None:
    # Inline suppression
    code_inline = """
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())  # dlt-doctor: ignore[PYSPARK-001] legacy third party library
def my_func(x):
    return str(x)
"""
    doctor = PlanDoctor()
    findings_inline = doctor.analyze_source_code(code_inline)
    assert len(findings_inline) == 0

    # File-level suppression
    code_file = """# dlt-doctor: ignore-file[PYSPARK-001,PYSPARK-006]
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def my_func(x):
    return str(x)

df = a.crossJoin(b)
"""
    findings_file = doctor.analyze_source_code(code_file)
    assert len(findings_file) == 0


def test_directory_report_output_formats() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        p1 = Path(tmpdir) / "p1.py"
        p1.write_text("df = a.crossJoin(b)\n", encoding="utf-8")

        scanner = DirectoryScanner()
        report = scanner.scan_directory(tmpdir)

        # Text format
        text = report.to_text()
        assert "DLT-Doctor Repository Audit" in text
        assert "PYSPARK-006" in text

        # Markdown format
        md = report.to_markdown()
        assert "## DLT-Doctor Multi-Pipeline Audit" in md
        assert "| `p1.py` |" in md

        # JSON format
        js = report.to_json()
        data = json.loads(js)
        assert data["total_files_scanned"] == 1
        assert data["flagged_files_count"] == 1

        # SARIF format
        sarif = report.to_sarif()
        sarif_data = json.loads(sarif)
        assert sarif_data["version"] == "2.1.0"
        assert len(sarif_data["runs"][0]["results"]) == 1
        assert sarif_data["runs"][0]["results"][0]["ruleId"] == "PYSPARK-006"

        # HTML format
        html_out = report.to_html()
        assert "<!DOCTYPE html>" in html_out
        assert "DLT-Doctor Audit:" in html_out
        assert "PYSPARK-006" in html_out
