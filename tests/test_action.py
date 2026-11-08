"""Tests for GitHub Action specification (action.yml) and action execution."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from dlt_doctor.action import run_action
from dlt_doctor.cli import main


def test_action_yml_structure() -> None:
    action_path = Path(__file__).parent.parent / "action.yml"
    assert action_path.exists(), "action.yml must exist at repository root"

    content = action_path.read_text(encoding="utf-8")

    # Verify inputs are defined
    required_inputs = [
        "path:",
        "format:",
        "fail-on:",
        "check-heal:",
        "config:",
        "sarif-output:",
        "python-version:",
        "working-directory:",
    ]
    for inp in required_inputs:
        assert inp in content, f"Missing input definition '{inp}' in action.yml"

    # Default fail-on must be CRITICAL
    assert 'default: "CRITICAL"' in content or "default: 'CRITICAL'" in content

    # Verify inputs are passed via environment variables
    assert "INPUT_PATH:" in content
    assert "INPUT_FAIL_ON:" in content
    assert "INPUT_FORMAT:" in content
    assert "INPUT_CONFIG:" in content
    assert "INPUT_SARIF_OUTPUT:" in content

    # Verify single action entrypoint
    assert "dlt_doctor.action" in content

    # Verify analyze step precedes heal check step
    analyze_pos = content.index("Run dlt-doctor analyze")
    heal_pos = content.index("Run dlt-doctor heal check")
    assert analyze_pos < heal_pos, "analyze step must run before heal check step"

    # Verify heal step conditions and safe environment variable passing
    assert "if: always() && inputs.check-heal == 'true'" in content
    assert "HEAL_PATH: ${{ inputs.path }}" in content
    assert "HEAL_CONFIG: ${{ inputs.config }}" in content

    # Verify no raw input injection in heal run script
    heal_section = content[heal_pos:]
    run_idx = heal_section.index("run: |")
    run_script = heal_section[run_idx:]
    assert "${{ inputs.path }}" not in run_script
    assert "${{ inputs.config }}" not in run_script


def test_action_clean_pipeline(tmp_path: Path) -> None:
    test_file = tmp_path / "clean_pipeline.py"
    test_file.write_text("df = spark.table('orders').select('id', 'amount')", encoding="utf-8")
    summary_file = tmp_path / "step_summary.md"

    env = {
        "INPUT_PATH": str(test_file),
        "INPUT_FORMAT": "text",
        "INPUT_FAIL_ON": "CRITICAL",
        "GITHUB_STEP_SUMMARY": str(summary_file),
    }

    with patch.dict("os.environ", env):
        exit_code = run_action()
        assert exit_code == 0

    assert summary_file.exists()
    summary_content = summary_file.read_text(encoding="utf-8")
    assert "DLT-Doctor Audit" in summary_content
    assert "100/100" in summary_content


def test_action_fail_on_thresholds(tmp_path: Path) -> None:
    bad_file = tmp_path / "bad.py"
    bad_file.write_text(
        """from pyspark.sql.functions import udf
@udf
def my_bad_udf(x):
    return x
""",
        encoding="utf-8",
    )

    # 1. fail-on CRITICAL fails with 1
    env_crit = {
        "INPUT_PATH": str(bad_file),
        "INPUT_FAIL_ON": "CRITICAL",
    }
    with patch.dict("os.environ", env_crit):
        assert run_action() == 1

    # 2. fail-on NONE exits with 0
    env_none = {
        "INPUT_PATH": str(bad_file),
        "INPUT_FAIL_ON": "NONE",
    }
    with patch.dict("os.environ", env_none):
        assert run_action() == 0


def test_action_sarif_output_generation(tmp_path: Path) -> None:
    bad_file = tmp_path / "bad.py"
    bad_file.write_text(
        """from pyspark.sql.functions import udf
@udf
def my_bad_udf(x):
    return x
""",
        encoding="utf-8",
    )
    sarif_file = tmp_path / "results.sarif"

    env = {
        "INPUT_PATH": str(bad_file),
        "INPUT_FAIL_ON": "NONE",
        "INPUT_SARIF_OUTPUT": str(sarif_file),
    }

    with patch.dict("os.environ", env):
        assert run_action() == 0

    assert sarif_file.exists()
    sarif = json.loads(sarif_file.read_text(encoding="utf-8"))
    assert sarif["version"] == "2.1.0"
    assert len(sarif["runs"]) == 1
    assert len(sarif["runs"][0]["results"]) >= 1
    assert sarif["runs"][0]["results"][0]["ruleId"] == "PYSPARK-001"


def test_action_custom_config_flag(tmp_path: Path) -> None:
    bad_file = tmp_path / "bad.py"
    bad_file.write_text(
        """from pyspark.sql.functions import udf
@udf
def my_bad_udf(x):
    return x
""",
        encoding="utf-8",
    )
    cfg_file = tmp_path / "custom_config.toml"
    cfg_file.write_text(
        """[tool.dlt-doctor]
ignore = ["PYSPARK-001"]
""",
        encoding="utf-8",
    )

    env = {
        "INPUT_PATH": str(bad_file),
        "INPUT_CONFIG": str(cfg_file),
        "INPUT_FAIL_ON": "CRITICAL",
    }
    with patch.dict("os.environ", env):
        # Ignored PYSPARK-001 -> 0 findings -> exits 0
        assert run_action() == 0


def test_action_check_heal_command(tmp_path: Path) -> None:
    """Action check-heal step runs dlt-doctor heal --check."""
    unhealed = tmp_path / "unhealed.py"
    unhealed.write_text(
        """import dlt
@dlt.table
@dlt.expect_or_drop("valid_val", "v > 0")
def t1():
    return dlt.read_stream("s1")
""",
        encoding="utf-8",
    )

    # heal --check exits 1 on unhealed
    with patch("sys.argv", ["dlt-doctor", "heal", str(unhealed), "--check"]):
        assert main() == 1

    # apply heal
    with patch("sys.argv", ["dlt-doctor", "heal", str(unhealed), "--write"]):
        assert main() == 0

    # heal --check exits 0 on healed
    with patch("sys.argv", ["dlt-doctor", "heal", str(unhealed), "--check"]):
        assert main() == 0


def test_action_heal_custom_config(tmp_path: Path) -> None:
    """Verify heal --config loads custom TOML config to suppress unhealed rules."""
    unhealed = tmp_path / "unhealed.py"
    unhealed.write_text(
        """import dlt
@dlt.table
@dlt.expect_or_drop("valid_val", "v > 0")
def t1():
    return dlt.read_stream("s1")
""",
        encoding="utf-8",
    )
    cfg_file = tmp_path / "custom.toml"
    cfg_file.write_text(
        """[tool.dlt-doctor]
ignore = ["DD001"]
""",
        encoding="utf-8",
    )

    # Without config: heal --check fails with 1
    with patch("sys.argv", ["dlt-doctor", "heal", str(unhealed), "--check"]):
        assert main() == 1

    # With config: heal --check ignores DD001 and succeeds with 0
    with patch(
        "sys.argv", ["dlt-doctor", "heal", str(unhealed), "--check", "--config", str(cfg_file)]
    ):
        assert main() == 0
