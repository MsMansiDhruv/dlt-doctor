"""Tests for recursive directory scanning and notebook AST auditing."""

from __future__ import annotations

import json
from pathlib import Path

from dlt_doctor.magic import audit_code_cell
from dlt_doctor.scanner import DirectoryScanner


def test_directory_scanner_discover_files(tmp_path: Path) -> None:
    # Setup directories
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "clean_pipeline.py").write_text("x = 1\n", encoding="utf-8")
    (src_dir / "notebook.ipynb").write_text(
        json.dumps({"cells": [{"cell_type": "code", "source": ["y = 2\n"]}]}),
        encoding="utf-8",
    )

    # Ignored directory
    venv_dir = tmp_path / ".venv" / "sub"
    venv_dir.mkdir(parents=True)
    (venv_dir / "ignored.py").write_text("z = 3\n", encoding="utf-8")

    scanner = DirectoryScanner()
    files = scanner.discover_files(tmp_path, recursive=True)
    file_names = [f.name for f in files]

    assert "clean_pipeline.py" in file_names
    assert "notebook.ipynb" in file_names
    assert "ignored.py" not in file_names


def test_directory_scanner_audit_report(tmp_path: Path) -> None:
    # 1 clean file
    clean_file = tmp_path / "clean.py"
    clean_file.write_text("df_clean = df.select('a', 'b')\n", encoding="utf-8")

    # 1 flagged file with chained withColumn
    flagged_file = tmp_path / "flagged.py"
    flagged_file.write_text(
        """
df_bad = (
    df.withColumn("c1", F.col("a"))
      .withColumn("c2", F.col("b"))
      .withColumn("c3", F.col("c"))
      .withColumn("c4", F.col("d"))
)
""",
        encoding="utf-8",
    )

    scanner = DirectoryScanner()
    report = scanner.scan_directory(tmp_path, recursive=True)

    assert report.total_files_scanned == 2
    assert report.clean_files_count == 1
    assert report.flagged_files_count == 1
    assert report.overall_health_score < 100
    assert len(report.all_findings) >= 1

    text_summary = report.to_text()
    assert "DLT-Doctor Repository Audit" in text_summary
    assert "clean.py" not in text_summary or "clean.py" in str(report.file_reports)
    assert "flagged.py" in text_summary

    md_summary = report.to_markdown()
    assert "## DLT-Doctor Multi-Pipeline Audit" in md_summary
    assert "| `flagged.py` |" in md_summary


def test_audit_code_cell() -> None:
    code = """
df_res = (
    df.withColumn("c1", F.col("a"))
      .withColumn("c2", F.col("b"))
      .withColumn("c3", F.col("c"))
      .withColumn("c4", F.col("d"))
)
"""
    html = audit_code_cell(code, cell_title="Orders Staging Cell")
    assert "Orders Staging Cell" in html
    assert "HEALTH SCORE" in html
