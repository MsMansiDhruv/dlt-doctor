"""Unit tests for dlt-doctor Model Context Protocol (MCP) stdio server."""

from __future__ import annotations

import io
import json
from pathlib import Path

from dlt_doctor.cli.main import build_parser
from dlt_doctor.mcp_server import SUPPORTED_PROTOCOL_VERSION, handle_message, serve_stdio


def test_initialize_and_ping() -> None:
    resp = handle_message({"jsonrpc": "2.0", "id": 1, "method": "initialize"})
    assert resp is not None
    assert resp["result"]["protocolVersion"] == SUPPORTED_PROTOCOL_VERSION
    assert resp["result"]["serverInfo"]["name"] == "dlt-doctor"
    assert "tools" in resp["result"]["capabilities"]
    assert "resources" in resp["result"]["capabilities"]

    ping_resp = handle_message({"jsonrpc": "2.0", "id": 2, "method": "ping"})
    assert ping_resp == {"jsonrpc": "2.0", "id": 2, "result": {}}


def test_notification_returns_none() -> None:
    resp = handle_message({"jsonrpc": "2.0", "method": "notifications/initialized"})
    assert resp is None


def test_tools_list_exposes_five_tools() -> None:
    resp = handle_message({"jsonrpc": "2.0", "id": 3, "method": "tools/list"})
    assert resp is not None
    tools = resp["result"]["tools"]
    names = {t["name"] for t in tools}
    assert names == {
        "analyze_pyspark_source",
        "analyze_sql_source",
        "scan_pipeline_directory",
        "forecast_dbu_cost",
        "audit_compaction_debt",
    }


def test_tool_analyze_pyspark_source() -> None:
    bad_code = (
        "import dlt\n"
        "@dlt.table\n"
        "@dlt.expect_or_drop('valid_id', 'id IS NOT NULL')\n"
        "def bronze():\n"
        "    return spark.read.parquet('/mnt/raw').collect()\n"
    )
    resp = handle_message(
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "analyze_pyspark_source",
                "arguments": {"source_code": bad_code, "file_name": "bronze.py"},
            },
        }
    )
    assert resp is not None
    assert resp["result"]["isError"] is False
    payload = json.loads(resp["result"]["content"][0]["text"])
    assert payload["file_name"] == "bronze.py"
    assert payload["finding_count"] >= 1


def test_tool_analyze_sql_source() -> None:
    sql_code = (
        "CREATE OR REFRESH STREAMING TABLE orders_clean (\n"
        "  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW\n"
        ") AS SELECT * FROM STREAM(LIVE.orders_raw);"
    )
    resp = handle_message(
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "analyze_sql_source",
                "arguments": {"sql_code": sql_code},
            },
        }
    )
    assert resp is not None
    assert resp["result"]["isError"] is False
    payload = json.loads(resp["result"]["content"][0]["text"])
    assert "findings" in payload


def test_tool_scan_pipeline_directory(tmp_path: Path) -> None:
    (tmp_path / "etl.py").write_text(
        "df = spark.read.table('orders')\nrows = df.collect()\n",
        encoding="utf-8",
    )
    resp = handle_message(
        {
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "scan_pipeline_directory",
                "arguments": {"directory_path": str(tmp_path)},
            },
        }
    )
    assert resp is not None
    assert resp["result"]["isError"] is False
    payload = json.loads(resp["result"]["content"][0]["text"])
    assert payload["total_files_scanned"] >= 1


def test_tool_forecast_dbu_cost() -> None:
    resp = handle_message(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "name": "forecast_dbu_cost",
                "arguments": {
                    "files_per_hour": 500,
                    "avg_file_mb": 20.0,
                    "is_continuous_streaming": True,
                },
            },
        }
    )
    assert resp is not None
    assert resp["result"]["isError"] is False
    payload = json.loads(resp["result"]["content"][0]["text"])
    assert payload["daily_ingestion_gb"] > 0
    assert payload["recommended_tier"]


def test_tool_audit_compaction_debt() -> None:
    small_files = [1_000_000] * 50  # 50 x 1MB files
    resp = handle_message(
        {
            "jsonrpc": "2.0",
            "id": 8,
            "method": "tools/call",
            "params": {
                "name": "audit_compaction_debt",
                "arguments": {
                    "table_name": "main.gold.orders",
                    "file_sizes_bytes": small_files,
                },
            },
        }
    )
    assert resp is not None
    assert resp["result"]["isError"] is False
    payload = json.loads(resp["result"]["content"][0]["text"])
    assert payload["has_compaction_debt"] is True
    assert "OPTIMIZE main.gold.orders" in payload["recommended_optimize_sql"]


def test_resources_list_and_read() -> None:
    list_resp = handle_message({"jsonrpc": "2.0", "id": 9, "method": "resources/list"})
    assert list_resp is not None
    assert list_resp["result"]["resources"][0]["uri"] == "dlt-doctor://rules"

    read_resp = handle_message(
        {
            "jsonrpc": "2.0",
            "id": 10,
            "method": "resources/read",
            "params": {"uri": "dlt-doctor://rules"},
        }
    )
    assert read_resp is not None
    rules = json.loads(read_resp["result"]["contents"][0]["text"])
    assert len(rules) >= 5


def test_error_handling_and_serve_stdio() -> None:
    unknown_tool = handle_message(
        {
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {"name": "nonexistent", "arguments": {}},
        }
    )
    assert unknown_tool is not None
    assert unknown_tool["error"]["code"] == -32602

    missing_arg = handle_message(
        {
            "jsonrpc": "2.0",
            "id": 12,
            "method": "tools/call",
            "params": {"name": "analyze_pyspark_source", "arguments": {}},
        }
    )
    assert missing_arg is not None
    assert missing_arg["result"]["isError"] is True

    stdin = io.StringIO(
        json.dumps({"jsonrpc": "2.0", "id": 1, "method": "ping"})
        + "\n{bad json\n"
        + json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"})
        + "\n"
    )
    stdout = io.StringIO()
    rc = serve_stdio(stdin=stdin, stdout=stdout)
    assert rc == 0
    lines = stdout.getvalue().strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["result"] == {}
    assert json.loads(lines[1])["error"]["code"] == -32700


def test_cli_mcp_subcommand_registered() -> None:
    args = build_parser().parse_args(["mcp"])
    assert args.command == "mcp"
