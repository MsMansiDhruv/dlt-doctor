"""Tests for smithery config, pre-commit hooks, multi-target analyze, and SARIF export."""

from __future__ import annotations

import json
from pathlib import Path
import pytest
import yaml

from dlt_doctor import __version__
from dlt_doctor.cli import main


def test_smithery_yaml_validity() -> None:
    smithery_path = Path(__file__).parent.parent / "smithery.yaml"
    assert smithery_path.exists(), "smithery.yaml must exist at repo root"
    content = smithery_path.read_text(encoding="utf-8")
    data = yaml.safe_load(content)

    assert data.get("startCommand", {}).get("type") == "stdio"
    cmd_fn = data.get("startCommand", {}).get("commandFunction")
    assert cmd_fn is not None
    assert "dlt-doctor" in str(cmd_fn)


def test_pre_commit_hooks_yaml_validity() -> None:
    hooks_path = Path(__file__).parent.parent / ".pre-commit-hooks.yaml"
    assert hooks_path.exists(), ".pre-commit-hooks.yaml must exist at repo root"
    content = hooks_path.read_text(encoding="utf-8")
    hooks = yaml.safe_load(content)

    assert isinstance(hooks, list)
    hook_ids = [h.get("id") for h in hooks]
    assert "dlt-doctor" in hook_ids
    assert "dlt-doctor-heal" in hook_ids

    analyze_hook = next(h for h in hooks if h.get("id") == "dlt-doctor")
    assert analyze_hook.get("entry") == "dlt-doctor analyze"
    assert analyze_hook.get("language") == "python"

    heal_hook = next(h for h in hooks if h.get("id") == "dlt-doctor-heal")
    assert "dlt-doctor heal" in heal_hook.get("entry")
    assert heal_hook.get("language") == "python"


def test_analyze_multi_file_targets(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    f1 = tmp_path / "clean_pipe.py"
    f1.write_text("df_clean = df.select('a', 'b')\n", encoding="utf-8")

    f2 = tmp_path / "cross_join.py"
    f2.write_text("df_bad = a.crossJoin(b)\n", encoding="utf-8")

    # Analyze both files together (simulating pre-commit passing multiple staged files)
    ret = main(["analyze", str(f1), str(f2), "--format", "text"])
    assert ret == 1  # f2 has CRITICAL/HIGH findings

    out = capsys.readouterr().out
    assert "DLT-Doctor Repository Audit" in out
    assert "flagged" in out.lower()


def test_analyze_multi_file_sarif_export(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    f1 = tmp_path / "pipe1.sql"
    f1.write_text("CREATE STREAMING TABLE bad1 AS SELECT * FROM tbl1, tbl2;\n", encoding="utf-8")

    f2 = tmp_path / "pipe2.sql"
    f2.write_text(
        "CREATE STREAMING TABLE bad2 (CONSTRAINT c1 EXPECT (ts > current_timestamp()) ON VIOLATION DROP ROW) AS SELECT 1;\n",
        encoding="utf-8",
    )

    ret = main(["analyze", str(f1), str(f2), "--format", "sarif"])
    assert ret == 1

    out = capsys.readouterr().out
    sarif = json.loads(out)
    assert sarif["version"] == "2.1.0"
    driver = sarif["runs"][0]["tool"]["driver"]
    assert driver["name"] == "dlt-doctor"
    assert driver["version"] == __version__
    assert "informationUri" in driver

    results = sarif["runs"][0]["results"]
    rule_ids = {r["ruleId"] for r in results}
    assert "PLAN-001" in rule_ids
    assert "DD004" in rule_ids


def test_heal_multi_file_targets(tmp_path: Path) -> None:
    f1 = tmp_path / "pipe1.py"
    f1.write_text(
        "import dlt\n@dlt.table\n@dlt.expect_or_drop('c', 'val > 0')\ndef t1():\n    return dlt.read_stream('src')\n",
        encoding="utf-8",
    )
    f2 = tmp_path / "pipe2.py"
    f2.write_text("x = 1\n", encoding="utf-8")

    # Multi-file check mode
    ret = main(["heal", str(f1), str(f2), "--check"])
    assert ret == 1  # f1 needs healing

    # Multi-file write mode
    ret_write = main(["heal", str(f1), str(f2), "--write"])
    assert ret_write == 0
    assert "_quarantine" in f1.read_text(encoding="utf-8")


def test_analyze_default_dot_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    clean = tmp_path / "clean.py"
    clean.write_text("a = 1\n", encoding="utf-8")

    # Calling analyze with zero path arguments should default to "."
    ret = main(["analyze", "--format", "json"])
    assert ret == 0
