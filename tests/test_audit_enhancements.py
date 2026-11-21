"""Unit tests for audit tightening: BOM handling, SQL Cartesian & nondet rules, checkpoint local scan, and CLI flag aliases."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch

from dlt_doctor.checkpoint import CheckpointDoctor, CheckpointIntegrityStatus
from dlt_doctor.cli import main
from dlt_doctor.expectations_rules import audit_sql_source
from dlt_doctor.heal import auto_heal_source, heal_file


def test_heal_bom_handling():
    """Verify that auto_heal_source and heal_file gracefully strip UTF-8 BOM."""
    raw_code = (
        "import dlt\n\n"
        "@dlt.table(name='silver_t')\n"
        "@dlt.expect_or_drop('valid_x', 'x > 0')\n"
        "def silver_t():\n"
        "    return dlt.read_stream('bronze')\n"
    )
    # Test auto_heal_source with BOM prefix
    code_with_bom = "\ufeff" + raw_code
    res = auto_heal_source(code_with_bom, file_path="test_bom.py")
    assert res.can_heal is True
    assert "silver_t_quarantine" in res.healed_code

    # Test heal_file with physical BOM bytes
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "pipeline_bom.py"
        test_file.write_bytes(b"\xef\xbb\xbf" + raw_code.encode("utf-8"))
        res_file = heal_file(test_file, write=True)
        assert res_file.can_heal is True
        assert test_file.exists()
        content = test_file.read_text(encoding="utf-8")
        assert "silver_t_quarantine" in content


def test_sql_cross_join_and_cartesian_detection():
    """Verify that audit_sql_source flags CROSS JOIN and implicit comma joins."""
    sql_cross = "SELECT * FROM orders CROSS JOIN customers;"
    findings = audit_sql_source(sql_cross)
    rule_ids = [f.rule_id for f in findings]
    assert "PLAN-001" in rule_ids
    assert any("CROSS JOIN" in f.title for f in findings)

    sql_implicit = "SELECT * FROM orders, customers;"
    findings_implicit = audit_sql_source(sql_implicit)
    assert any(f.rule_id == "PLAN-001" for f in findings_implicit)


def test_sql_nondeterministic_expectation():
    """Verify that audit_sql_source flags non-deterministic functions in expectations."""
    sql = "CREATE TABLE silver (CONSTRAINT valid_t EXPECT (event_time <= current_timestamp()));"
    findings = audit_sql_source(sql)
    assert any(f.rule_id == "DD004" for f in findings)


def test_checkpoint_local_inspection():
    """Verify that CheckpointDoctor inspects real offsets and commits if local directory exists."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        offsets = base / "offsets"
        commits = base / "commits"
        state = base / "state"
        offsets.mkdir()
        commits.mkdir()
        state.mkdir()

        for i in range(11):
            (offsets / str(i)).write_text("{}", encoding="utf-8")
        for i in range(10):
            (commits / str(i)).write_text("{}", encoding="utf-8")

        doctor = CheckpointDoctor()
        rep = doctor.audit_checkpoint_state(str(base))
        assert rep.has_stateful_operators is True
        assert rep.status == CheckpointIntegrityStatus.SUSPECT
        assert rep.latest_batch_id == 9
        assert rep.uncommitted_batches == [10]


def test_cli_aliases():
    """Verify that CLI flag aliases work (--total-rows, --retention-hours, --shuffle-joins, --threshold-mb)."""
    with patch(
        "sys.argv",
        [
            "dlt-doctor",
            "cluster",
            "--table",
            "main.gold.orders",
            "--cols",
            "cust_id:string:1000:0.8",
            "--total-rows",
            "500000",
        ],
    ):
        assert main() == 0

    with patch(
        "sys.argv",
        [
            "dlt-doctor",
            "vacuum",
            "--table",
            "main.silver.orders",
            "--retention-hours",
            "24",
            "--readers",
            "1",
        ],
    ):
        # 24h with active readers is DANGEROUS, CLI correctly returns non-zero safety exit code 1
        assert main() == 1

    with patch(
        "sys.argv",
        [
            "dlt-doctor",
            "cost",
            "--files-per-hour",
            "10000",
            "--avg-file-mb",
            "8.0",
            "--shuffle-joins",
            "2",
        ],
    ):
        assert main() == 0

    with patch(
        "sys.argv",
        [
            "dlt-doctor",
            "compact",
            "--table",
            "main.gold.orders",
            "--threshold-mb",
            "64",
        ],
    ):
        assert main() == 0
