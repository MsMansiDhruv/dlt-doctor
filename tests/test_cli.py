"""Tests for dlt-doctor command line interface."""

from pathlib import Path
from unittest.mock import patch

from dlt_doctor.cli import main


def test_cli_analyze_clean_file(tmp_path: Path) -> None:
    test_file = tmp_path / "clean_pipeline.py"
    test_file.write_text(
        "df = spark.table('orders').select('id', 'amount')", encoding="utf-8"
    )

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
