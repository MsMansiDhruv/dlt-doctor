"""Tests for Databricks workspace telemetry connector."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dlt_doctor.cli import main
from dlt_doctor.connect import WorkspaceInspector


def test_workspace_inspector_mock_diagnosis() -> None:
    inspector = WorkspaceInspector()
    mock_detail = {
        "numFiles": 5000,
        "sizeInBytes": 20 * 1024 * 1024 * 1024,  # 20 GB (avg 4 MB -> fragmented)
        "clusteringColumns": ["customer_id", "order_date"],
        "partitionColumns": [],
    }
    mock_history = [{"version": i} for i in range(48)]  # 48 commits in 24h = 2/hr

    report = inspector.inspect_table(
        table_name="main.gold.fact_orders",
        mock_detail=mock_detail,
        mock_history=mock_history,
    )

    assert report.table_name == "main.gold.fact_orders"
    assert report.num_files == 5000
    assert "HIGH" in report.compaction_debt_status
    assert "OPTIMIZE main.gold.fact_orders" in report.compaction_recommended_sql
    assert "CLUSTER BY" in report.compaction_recommended_sql
    assert report.commits_last_24h == 48

    text_output = report.to_text()
    assert "Databricks Live Table Telemetry" in text_output
    assert "Clustering: ['customer_id', 'order_date']" in text_output


def test_workspace_inspector_missing_credentials() -> None:
    inspector = WorkspaceInspector(host="", token="", warehouse_id="")
    with pytest.raises(ValueError, match="Missing Databricks connection credentials"):
        inspector.execute_sql("SELECT 1")


def test_cli_inspect_table_mock() -> None:
    test_args = [
        "dlt-doctor",
        "inspect-table",
        "main.gold.orders",
        "--mock",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0
