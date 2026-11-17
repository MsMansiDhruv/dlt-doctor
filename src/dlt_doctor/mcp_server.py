"""Model Context Protocol (MCP) stdio server for dlt-doctor."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import IO, Any

from dlt_doctor import __version__
from dlt_doctor.catalyst import PlanDoctor
from dlt_doctor.compaction import CompactionDoctor
from dlt_doctor.cost import CostDoctor
from dlt_doctor.expectations_rules import audit_sql_source
from dlt_doctor.rules import list_rules
from dlt_doctor.scanner import DirectoryScanner

SUPPORTED_PROTOCOL_VERSION = "2024-11-05"

ToolHandler = Callable[[dict[str, Any]], dict[str, Any]]

_TOOLS_SPEC: list[dict[str, Any]] = [
    {
        "name": "analyze_pyspark_source",
        "description": (
            "Statically analyze PySpark or Delta Live Tables (DLT) Python source code "
            "for Catalyst plan bloat, anti-patterns, skew risks, and missing quarantine gates."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "source_code": {
                    "type": "string",
                    "description": "Python source code of the PySpark or DLT pipeline.",
                },
                "file_name": {
                    "type": "string",
                    "description": "Optional filename context (default: pipeline.py).",
                    "default": "pipeline.py",
                },
            },
            "required": ["source_code"],
        },
    },
    {
        "name": "analyze_sql_source",
        "description": (
            "Audit Databricks SQL or Delta Live Tables SQL source code for quality "
            "expectation risks, schema issues, and optimization opportunities."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "sql_code": {
                    "type": "string",
                    "description": "Databricks SQL / DLT SQL query text to audit.",
                },
            },
            "required": ["sql_code"],
        },
    },
    {
        "name": "scan_pipeline_directory",
        "description": (
            "Recursively scan a local directory of PySpark, SQL, and notebook files "
            "and return a structured JSON audit report."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "directory_path": {
                    "type": "string",
                    "description": "Local filesystem path to the pipeline repository or folder.",
                },
                "recursive": {
                    "type": "boolean",
                    "description": "Whether to scan subdirectories recursively (default: true).",
                    "default": True,
                },
            },
            "required": ["directory_path"],
        },
    },
    {
        "name": "forecast_dbu_cost",
        "description": (
            "Forecast Classic vs Serverless Databricks DBU consumption and recommend "
            "worker sizing from ingestion throughput and shuffle complexity."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "files_per_hour": {
                    "type": "integer",
                    "description": "Estimated incoming files per hour.",
                },
                "avg_file_mb": {
                    "type": "number",
                    "description": "Average incoming file size in MB (default: 10.0).",
                    "default": 10.0,
                },
                "is_continuous_streaming": {
                    "type": "boolean",
                    "description": "True for continuous streaming, False for triggered batch.",
                    "default": True,
                },
                "active_hours_per_day": {
                    "type": "number",
                    "description": "Active compute hours per day (default: 24.0).",
                    "default": 24.0,
                },
                "shuffle_join_count": {
                    "type": "integer",
                    "description": "Number of wide shuffle/join stages (default: 1).",
                    "default": 1,
                },
            },
            "required": ["files_per_hour"],
        },
    },
    {
        "name": "audit_compaction_debt",
        "description": (
            "Evaluate Delta Lake table file-size distribution for small-file "
            "fragmentation and generate targeted OPTIMIZE / TBLPROPERTIES SQL."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Three-part Unity Catalog or Delta table name.",
                },
                "file_sizes_bytes": {
                    "type": "array",
                    "items": {"type": "integer"},
                    "description": "Array of Parquet data file sizes in bytes from Delta log.",
                },
                "small_file_threshold_mb": {
                    "type": "integer",
                    "description": "Threshold in MB below which files count as small (default: 32).",
                    "default": 32,
                },
            },
            "required": ["table_name", "file_sizes_bytes"],
        },
    },
]


def _tool_analyze_pyspark_source(args: dict[str, Any]) -> dict[str, Any]:
    source_code = str(args["source_code"])
    file_name = str(args.get("file_name", "pipeline.py"))
    findings = PlanDoctor().analyze_source_code(source_code, file_name=file_name)
    return {
        "file_name": file_name,
        "finding_count": len(findings),
        "findings": [f.to_dict() for f in findings],
    }


def _tool_analyze_sql_source(args: dict[str, Any]) -> dict[str, Any]:
    sql_code = str(args["sql_code"])
    findings = audit_sql_source(sql_code)
    return {
        "finding_count": len(findings),
        "findings": [f.to_dict() for f in findings],
    }


def _tool_scan_pipeline_directory(args: dict[str, Any]) -> dict[str, Any]:
    target = Path(str(args["directory_path"]))
    if not target.exists() or not target.is_dir():
        raise ValueError(f"Directory not found: {target}")
    recursive = bool(args.get("recursive", True))
    report = DirectoryScanner().scan_directory(target, recursive=recursive)
    return json.loads(report.to_json())  # type: ignore[no-any-return]


def _tool_forecast_dbu_cost(args: dict[str, Any]) -> dict[str, Any]:
    forecast = CostDoctor().forecast_pipeline_cost(
        files_per_hour=int(args["files_per_hour"]),
        avg_file_mb=float(args.get("avg_file_mb", 10.0)),
        is_continuous_streaming=bool(args.get("is_continuous_streaming", True)),
        active_hours_per_day=float(args.get("active_hours_per_day", 24.0)),
        shuffle_join_count=int(args.get("shuffle_join_count", 1)),
    )
    return asdict(forecast)


def _tool_audit_compaction_debt(args: dict[str, Any]) -> dict[str, Any]:
    table_name = str(args["table_name"])
    file_sizes = [int(x) for x in args["file_sizes_bytes"]]
    threshold_mb = int(args.get("small_file_threshold_mb", 32))
    report = CompactionDoctor().analyze_file_distribution(
        table_name=table_name,
        file_sizes_bytes=file_sizes,
        small_file_threshold_mb=threshold_mb,
    )
    return report.to_dict()


_TOOL_HANDLERS: dict[str, ToolHandler] = {
    "analyze_pyspark_source": _tool_analyze_pyspark_source,
    "analyze_sql_source": _tool_analyze_sql_source,
    "scan_pipeline_directory": _tool_scan_pipeline_directory,
    "forecast_dbu_cost": _tool_forecast_dbu_cost,
    "audit_compaction_debt": _tool_audit_compaction_debt,
}


def _jsonrpc_result(req_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _jsonrpc_error(req_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def handle_message(message: dict[str, Any]) -> dict[str, Any] | None:
    """Process a single JSON-RPC 2.0 MCP request and return a response (or None for notifications)."""
    method = message.get("method")
    req_id = message.get("id")
    params = message.get("params") or {}

    if not isinstance(method, str):
        return _jsonrpc_error(req_id, -32600, "Invalid Request: missing method")

    # Notifications carry no id and expect no response
    if req_id is None:
        return None

    if method == "initialize":
        return _jsonrpc_result(
            req_id,
            {
                "protocolVersion": SUPPORTED_PROTOCOL_VERSION,
                "capabilities": {
                    "tools": {"listChanged": False},
                    "resources": {"subscribe": False, "listChanged": False},
                },
                "serverInfo": {
                    "name": "dlt-doctor",
                    "version": __version__,
                },
            },
        )

    if method == "ping":
        return _jsonrpc_result(req_id, {})

    if method == "tools/list":
        return _jsonrpc_result(req_id, {"tools": _TOOLS_SPEC})

    if method == "tools/call":
        tool_name = params.get("name")
        tool_args = params.get("arguments") or {}
        if not isinstance(tool_name, str) or tool_name not in _TOOL_HANDLERS:
            return _jsonrpc_error(req_id, -32602, f"Unknown tool: {tool_name}")
        if not isinstance(tool_args, dict):
            return _jsonrpc_error(req_id, -32602, "Tool arguments must be an object")
        try:
            payload = _TOOL_HANDLERS[tool_name](tool_args)
            return _jsonrpc_result(
                req_id,
                {
                    "content": [{"type": "text", "text": json.dumps(payload, indent=2)}],
                    "isError": False,
                },
            )
        except (KeyError, TypeError, ValueError) as exc:
            return _jsonrpc_result(
                req_id,
                {
                    "content": [{"type": "text", "text": f"Tool error: {exc}"}],
                    "isError": True,
                },
            )

    if method == "resources/list":
        return _jsonrpc_result(
            req_id,
            {
                "resources": [
                    {
                        "uri": "dlt-doctor://rules",
                        "name": "Registered Diagnostic Rules",
                        "description": "Catalog of all static AST and Lakehouse diagnostic rules in dlt-doctor.",
                        "mimeType": "application/json",
                    }
                ]
            },
        )

    if method == "resources/read":
        uri = params.get("uri")
        if uri != "dlt-doctor://rules":
            return _jsonrpc_error(req_id, -32602, f"Unknown resource URI: {uri}")
        rules_payload = [
            {
                "id": r.id,
                "name": r.name,
                "severity": r.default_severity.value,
                "description": r.description,
            }
            for r in list_rules()
        ]
        return _jsonrpc_result(
            req_id,
            {
                "contents": [
                    {
                        "uri": uri,
                        "mimeType": "application/json",
                        "text": json.dumps(rules_payload, indent=2),
                    }
                ]
            },
        )

    return _jsonrpc_error(req_id, -32601, f"Method not found: {method}")


def serve_stdio(
    stdin: IO[str] | None = None,
    stdout: IO[str] | None = None,
) -> int:
    """Run the newline-delimited JSON-RPC 2.0 MCP server over stdio."""
    in_stream = stdin if stdin is not None else sys.stdin
    out_stream = stdout if stdout is not None else sys.stdout

    for raw_line in in_stream:
        line = raw_line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError as exc:
            err = _jsonrpc_error(None, -32700, f"Parse error: {exc}")
            out_stream.write(json.dumps(err) + "\n")
            out_stream.flush()
            continue

        if not isinstance(msg, dict):
            err = _jsonrpc_error(None, -32600, "Invalid Request: expected JSON object")
            out_stream.write(json.dumps(err) + "\n")
            out_stream.flush()
            continue

        response = handle_message(msg)
        if response is not None:
            out_stream.write(json.dumps(response) + "\n")
            out_stream.flush()

    return 0
