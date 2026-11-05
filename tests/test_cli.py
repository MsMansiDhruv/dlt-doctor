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


def test_cli_analyze_auto_heal_and_dry_run(tmp_path: Path) -> None:
    code = """import dlt
@dlt.table(name="test_heal_tbl")
@dlt.expect_or_drop("valid_id", "id > 0")
def test_heal_tbl():
    return dlt.read_stream("bronze")
"""
    pipeline_file = tmp_path / "heal_pipeline.py"
    pipeline_file.write_text(code, encoding="utf-8")

    # Dry run should not modify file
    test_args = ["dlt-doctor", "analyze", str(pipeline_file), "--auto-heal", "--dry-run"]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0
    assert pipeline_file.read_text(encoding="utf-8") == code

    # Without dry run, modifies file
    test_args_apply = ["dlt-doctor", "analyze", str(pipeline_file), "--auto-heal"]
    with patch("sys.argv", test_args_apply):
        exit_code = main()
        assert exit_code == 0
    assert "_failed_rules" in pipeline_file.read_text(encoding="utf-8")


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
