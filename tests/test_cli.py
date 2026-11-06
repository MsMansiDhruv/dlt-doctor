"""Tests for dlt-doctor command line interface."""

from pathlib import Path
from unittest.mock import patch

from dlt_doctor.cli import main


def test_cli_analyze_clean_file(tmp_path: Path) -> None:
    test_file = tmp_path / "clean_pipeline.py"
    test_file.write_text("df = spark.table('orders').select('id', 'amount')", encoding="utf-8")

    test_args = ["dlt-doctor", "analyze", str(test_file)]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_analyze_critical_udf(tmp_path: Path) -> None:
    test_file = tmp_path / "bad_pipeline.py"
    test_file.write_text(
        """
from pyspark.sql.functions import udf
@udf
def clean_it(x):
    return x
""",
        encoding="utf-8",
    )

    test_args = ["dlt-doctor", "analyze", str(test_file)]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 1


def test_cli_restate() -> None:
    test_args = [
        "dlt-doctor",
        "restate",
        "--table",
        "main.gold.fact_orders",
        "--late-date",
        "2026-10-25",
        "--window",
        "7",
        "--source",
        "source_recalculated",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_cdc() -> None:
    test_args = [
        "dlt-doctor",
        "cdc",
        "--table",
        "main.silver.orders",
        "--keys",
        "order_id",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_compact() -> None:
    test_args = [
        "dlt-doctor",
        "compact",
        "--table",
        "main.gold.orders",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_uc_migrate(tmp_path: Path) -> None:
    test_file = tmp_path / "legacy.py"
    test_file.write_text("df = spark.read.json('/mnt/raw/orders')", encoding="utf-8")

    test_args = ["dlt-doctor", "uc-migrate", str(test_file)]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 1  # critical finding for dbfs mount


def test_cli_cluster() -> None:
    test_args = [
        "dlt-doctor",
        "cluster",
        "--table",
        "main.gold.orders",
        "--cols",
        "customer_id",
        "order_date",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_vacuum() -> None:
    test_args = [
        "dlt-doctor",
        "vacuum",
        "--table",
        "main.silver.orders",
        "--retention",
        "168",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_schema() -> None:
    test_args = [
        "dlt-doctor",
        "schema",
        "--table",
        "main.silver.orders",
        "--incoming-cols",
        "id:bigint",
        "email:string",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_cost() -> None:
    test_args = [
        "dlt-doctor",
        "cost",
        "--files-per-hour",
        "1000",
        "--avg-file-mb",
        "5.0",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_spill() -> None:
    test_args = [
        "dlt-doctor",
        "spill",
        "--dataset-gb",
        "50.0",
        "--cores",
        "16",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_history() -> None:
    test_args = [
        "dlt-doctor",
        "history",
        "--table",
        "main.silver.orders",
        "--commits-per-hour",
        "60",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_checkpoint() -> None:
    test_args = [
        "dlt-doctor",
        "checkpoint",
        "--dir",
        "/Volumes/checkpoints/orders",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_analyze_directory(tmp_path: Path) -> None:
    pipeline_dir = tmp_path / "pipelines"
    pipeline_dir.mkdir()
    (pipeline_dir / "p1.py").write_text("df = spark.table('t1')", encoding="utf-8")
    (pipeline_dir / "p2.py").write_text("df = spark.table('t2')", encoding="utf-8")

    test_args = ["dlt-doctor", "analyze", str(pipeline_dir)]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_analyze_notebook(tmp_path: Path) -> None:
    import json

    nb_file = tmp_path / "orders.ipynb"
    nb_file.write_text(
        json.dumps({"cells": [{"cell_type": "code", "source": ["df = spark.table('orders')\n"]}]}),
        encoding="utf-8",
    )

    test_args = ["dlt-doctor", "analyze", str(nb_file)]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_analyze_auto_heal_preview_and_write(tmp_path: Path) -> None:
    code = """import dlt
@dlt.table(name="test_heal_tbl")
@dlt.expect_or_drop("valid_id", "id > 0")
def test_heal_tbl():
    return dlt.read_stream("bronze")
"""
    pipeline_file = tmp_path / "heal_pipeline.py"
    pipeline_file.write_text(code, encoding="utf-8")

    # Preview by default (does not modify file)
    test_args = ["dlt-doctor", "analyze", str(pipeline_file), "--auto-heal"]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0
    assert pipeline_file.read_text(encoding="utf-8") == code

    # With --write and --quarantine-suffix, modifies file
    test_args_apply = [
        "dlt-doctor",
        "analyze",
        str(pipeline_file),
        "--auto-heal",
        "--write",
        "--quarantine-suffix",
        "_dlq",
    ]
    with patch("sys.argv", test_args_apply):
        exit_code = main()
        assert exit_code == 0
    healed = pipeline_file.read_text(encoding="utf-8")
    assert "_failed_rules" in healed
    assert "test_heal_tbl_dlq" in healed


def test_cli_analyze_auto_heal_refusal_exits_nonzero(tmp_path: Path) -> None:
    code = """import dlt
@dlt.table
@dlt.expect_all_or_drop(fetch_remote_rules())
def dynamic_pipe():
    return dlt.read_stream("bronze")
"""
    pipeline_file = tmp_path / "dynamic.py"
    pipeline_file.write_text(code, encoding="utf-8")

    test_args = ["dlt-doctor", "analyze", str(pipeline_file), "--auto-heal"]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code != 0


def test_cli_heal_check_and_custom_suffix(tmp_path: Path) -> None:
    code = """import dlt
@dlt.table(name="users")
@dlt.expect_or_drop("valid_u", "id IS NOT NULL")
def users():
    return dlt.read_stream("raw")
"""
    pipeline_file = tmp_path / "users.py"
    pipeline_file.write_text(code, encoding="utf-8")

    # --check exits 1 when healing is needed
    check_args = ["dlt-doctor", "heal", str(pipeline_file), "--check"]
    with patch("sys.argv", check_args):
        exit_code = main()
        assert exit_code == 1

    # heal with custom suffix
    write_args = [
        "dlt-doctor",
        "heal",
        str(pipeline_file),
        "--write",
        "--quarantine-suffix",
        "_dlq",
    ]
    with patch("sys.argv", write_args):
        exit_code = main()
        assert exit_code == 0
    healed = pipeline_file.read_text(encoding="utf-8")
    assert "users_dlq" in healed

    # Running --check on healed file now exits 0
    with patch("sys.argv", check_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_heal_exit_code_3_cannot_heal(tmp_path: Path) -> None:
    """When a file has dynamic expectations that cannot be resolved, exit code is 3."""
    code = """import dlt
@dlt.table
@dlt.expect_all_or_drop(fetch_remote_rules())
def dynamic_pipe():
    return dlt.read_stream("bronze")
"""
    pipeline_file = tmp_path / "unhealable.py"
    pipeline_file.write_text(code, encoding="utf-8")

    check_args = ["dlt-doctor", "heal", str(pipeline_file), "--check"]
    with patch("sys.argv", check_args):
        exit_code = main()
        assert exit_code == 3


def test_cli_heal_directory_support(tmp_path: Path) -> None:
    """heal command must support directories for CI gates."""
    pipe_dir = tmp_path / "pipes"
    pipe_dir.mkdir()
    (pipe_dir / "p1.py").write_text(
        """import dlt
@dlt.table
@dlt.expect_or_drop("v1", "x > 0")
def p1():
    return dlt.read_stream("s1")
""",
        encoding="utf-8",
    )
    (pipe_dir / "p2.py").write_text("import dlt\n# already compliant", encoding="utf-8")

    # In check mode, directory with unhealed file exits 1
    with patch("sys.argv", ["dlt-doctor", "heal", str(pipe_dir), "--check"]):
        assert main() == 1

    # In write mode, directory heals files and exits 0
    with patch("sys.argv", ["dlt-doctor", "heal", str(pipe_dir), "--write"]):
        assert main() == 0

    # Running --check on now-healed directory exits 0
    with patch("sys.argv", ["dlt-doctor", "heal", str(pipe_dir), "--check"]):
        assert main() == 0
