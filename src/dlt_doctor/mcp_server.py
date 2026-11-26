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
            "for Catalyst plan bloat, anti-patterns, skew risks, watermark reordering, and missing quarantine gates.\n\n"
            "Usage Guidelines:\n"
            "- Use for individual Python (.py) source strings or pipeline scripts.\n"
            "- For SQL queries or DLT SQL statements, use analyze_sql_source instead.\n"
            "- For scanning entire directories or multiple repository files, use scan_pipeline_directory instead.\n\n"
            "Behavior:\n"
            "- Runs 100% offline via local Python AST inspection. Does NOT connect to Databricks clusters, execute code, or require JVM/cloud credentials.\n"
            "- Deterministic and idempotent. Returns a structured JSON report containing finding counts and diagnostic items.\n"
            "- Syntax errors or invalid Python constructs are surfaced as diagnostic findings rather than raising unhandled exceptions."
        ),
        "annotations": {
            "readOnlyHint": True,
            "idempotentHint": True,
            "openWorldHint": False,
        },
        "inputSchema": {
            "type": "object",
            "properties": {
                "source_code": {
                    "type": "string",
                    "description": "Python source code of the PySpark or DLT pipeline to audit.",
                },
                "file_name": {
                    "type": "string",
                    "description": "Optional contextual filename for reporting (default: pipeline.py).",
                    "default": "pipeline.py",
                },
            },
            "required": ["source_code"],
        },
        "outputSchema": {
            "type": "object",
            "properties": {
                "file_name": {
                    "type": "string",
                    "description": "Contextual filename that was audited.",
                },
                "finding_count": {
                    "type": "integer",
                    "description": "Total number of diagnostic findings discovered.",
                },
                "findings": {
                    "type": "array",
                    "description": "List of diagnostic issues detected in the PySpark source.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "rule_id": {
                                "type": "string",
                                "description": "Diagnostic rule ID (e.g., DD001, DD002, PERF001).",
                            },
                            "rule_name": {
                                "type": "string",
                                "description": "Human-readable diagnostic rule name.",
                            },
                            "severity": {
                                "type": "string",
                                "enum": ["HIGH", "MEDIUM", "LOW", "INFO"],
                                "description": "Diagnostic severity level.",
                            },
                            "line_number": {
                                "type": "integer",
                                "description": "1-based line number in source code.",
                            },
                            "message": {
                                "type": "string",
                                "description": "Detailed explanation of the issue and production risk.",
                            },
                            "remediation": {
                                "type": "string",
                                "description": "Prescriptive remediation guidance or code fix.",
                            },
                        },
                        "required": ["rule_id", "severity", "message"],
                    },
                },
            },
            "required": ["file_name", "finding_count", "findings"],
        },
    },
    {
        "name": "analyze_sql_source",
        "description": (
            "Audit Databricks SQL or Delta Live Tables (DLT) SQL source code for quality "
            "expectation risks, Cartesian joins, schema drift traps, and optimization opportunities.\n\n"
            "Usage Guidelines:\n"
            "- Use for individual SQL (.sql) query text or DLT SQL statements (e.g., CREATE STREAMING TABLE, expect_or_drop).\n"
            "- For PySpark Python pipelines, use analyze_pyspark_source instead.\n"
            "- For scanning an entire repository or folder of mixed SQL and Python files, use scan_pipeline_directory instead.\n\n"
            "Behavior:\n"
            "- Pure offline static AST parsing via sqlglot. Does NOT execute queries, access databases, or connect to Databricks SQL warehouses.\n"
            "- Deterministic and idempotent. Returns a structured diagnostic report with violation counts and remediation advice.\n"
            "- Syntax errors are surfaced as structured diagnostic findings."
        ),
        "annotations": {
            "readOnlyHint": True,
            "idempotentHint": True,
            "openWorldHint": False,
        },
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
        "outputSchema": {
            "type": "object",
            "properties": {
                "finding_count": {
                    "type": "integer",
                    "description": "Total number of SQL diagnostic findings discovered.",
                },
                "findings": {
                    "type": "array",
                    "description": "List of SQL quality and performance findings.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "rule_id": {
                                "type": "string",
                                "description": "Rule identifier (e.g., PLAN-001, DD001).",
                            },
                            "rule_name": {
                                "type": "string",
                                "description": "Diagnostic rule name.",
                            },
                            "severity": {
                                "type": "string",
                                "enum": ["HIGH", "MEDIUM", "LOW", "INFO"],
                                "description": "Diagnostic severity level.",
                            },
                            "line_number": {
                                "type": "integer",
                                "description": "Line number where the SQL construct was detected.",
                            },
                            "message": {
                                "type": "string",
                                "description": "Description of the anti-pattern or risk.",
                            },
                            "remediation": {
                                "type": "string",
                                "description": "Actionable SQL fix recommendation.",
                            },
                        },
                        "required": ["rule_id", "severity", "message"],
                    },
                },
            },
            "required": ["finding_count", "findings"],
        },
    },
    {
        "name": "scan_pipeline_directory",
        "description": (
            "Recursively scan a local repository or directory of PySpark (.py), SQL (.sql), "
            "and Databricks notebook (.ipynb) files to produce an aggregated lakehouse quality audit report.\n\n"
            "Usage Guidelines:\n"
            "- Use to audit an entire repository, dbt/DLT project, or folder containing multiple pipeline files.\n"
            "- For auditing a single Python pipeline script, prefer analyze_pyspark_source.\n"
            "- For auditing an isolated SQL query, prefer analyze_sql_source.\n\n"
            "Behavior:\n"
            "- Recursively inspects the filesystem at directory_path.\n"
            "- Completely offline; runs without cloud credentials, JVM, or active cluster connections.\n"
            "- Evaluates all supported files, computes individual and aggregate health scores (0-100), and reports per-file findings.\n"
            "- Individual file errors are captured inside per-file reports without halting the directory scan."
        ),
        "annotations": {
            "readOnlyHint": True,
            "idempotentHint": True,
            "openWorldHint": False,
        },
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
        "outputSchema": {
            "type": "object",
            "properties": {
                "target_directory": {
                    "type": "string",
                    "description": "Directory path that was audited.",
                },
                "total_files_scanned": {
                    "type": "integer",
                    "description": "Total pipeline files analyzed.",
                },
                "clean_files": {
                    "type": "integer",
                    "description": "Number of files with zero findings.",
                },
                "flagged_files": {
                    "type": "integer",
                    "description": "Number of files with at least one finding.",
                },
                "overall_score": {
                    "type": "number",
                    "description": "Composite health score from 0 (worst) to 100 (clean).",
                },
                "reports": {
                    "type": "array",
                    "description": "Per-file audit reports.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "file_path": {
                                "type": "string",
                                "description": "Relative or absolute path to the file.",
                            },
                            "finding_count": {
                                "type": "integer",
                                "description": "Number of findings in this file.",
                            },
                            "findings": {
                                "type": "array",
                                "description": "List of diagnostic findings for this file.",
                            },
                        },
                        "required": ["file_path", "finding_count"],
                    },
                },
            },
            "required": ["total_files_scanned", "clean_files", "flagged_files", "overall_score"],
        },
    },
    {
        "name": "forecast_dbu_cost",
        "description": (
            "Forecast Databricks DBU consumption and compute costs (Classic compute vs. Serverless) "
            "and recommend optimal worker sizing based on ingestion throughput and shuffle complexity.\n\n"
            "Usage Guidelines:\n"
            "- Use during architectural sizing, capacity planning, and cost optimization before deploying Lakeflow or DLT pipelines.\n"
            "- For Delta Lake table storage layout and file size optimization, use audit_compaction_debt instead.\n\n"
            "Behavior:\n"
            "- Pure deterministic mathematical calculation based on Databricks official DBU pricing and node SKU specifications.\n"
            "- Does NOT query Databricks billing APIs, account consoles, or live workspace compute.\n"
            "- Returns hourly, daily, and monthly DBU and USD cost projections across Classic and Serverless options with cluster node recommendations."
        ),
        "annotations": {
            "readOnlyHint": True,
            "idempotentHint": True,
            "openWorldHint": False,
        },
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
        "outputSchema": {
            "type": "object",
            "properties": {
                "estimated_daily_gb": {
                    "type": "number",
                    "description": "Estimated daily volume ingested in gigabytes.",
                },
                "recommended_worker_count": {
                    "type": "integer",
                    "description": "Recommended number of worker nodes.",
                },
                "recommended_worker_type": {
                    "type": "string",
                    "description": "Recommended cloud worker SKU (e.g., i3.xlarge).",
                },
                "classic_daily_dbu": {
                    "type": "number",
                    "description": "Projected daily DBU burn on Classic compute.",
                },
                "classic_monthly_cost_usd": {
                    "type": "number",
                    "description": "Projected monthly USD cost on Classic compute.",
                },
                "serverless_daily_dbu": {
                    "type": "number",
                    "description": "Projected daily DBU burn on Serverless compute.",
                },
                "serverless_monthly_cost_usd": {
                    "type": "number",
                    "description": "Projected monthly USD cost on Serverless compute.",
                },
            },
            "required": [
                "recommended_worker_count",
                "classic_daily_dbu",
                "classic_monthly_cost_usd",
            ],
        },
    },
    {
        "name": "audit_compaction_debt",
        "description": (
            "Evaluate Delta Lake table file-size distribution for small-file fragmentation debt "
            "and synthesize targeted OPTIMIZE, ZORDER BY, Liquid Clustering, and TBLPROPERTIES SQL commands.\n\n"
            "Usage Guidelines:\n"
            "- Use when troubleshooting slow Delta Lake queries, read amplification, or excessive metadata overhead.\n"
            "- Prerequisites: Obtain file sizes in bytes (e.g. from DESCRIBE DETAIL table_name, Delta transaction logs, or cloud storage inventory).\n"
            "- To forecast pipeline compute cluster DBU burn instead, use forecast_dbu_cost.\n\n"
            "Behavior:\n"
            "- Purely analytical calculation; parses file size array and computes fragmentation statistics.\n"
            "- Does NOT connect to cloud object storage or execute the generated OPTIMIZE/ALTER TABLE statements against a live warehouse.\n"
            "- Returns small-file ratios, compaction urgency rating, and copy-pasteable SQL remediation scripts."
        ),
        "annotations": {
            "readOnlyHint": True,
            "idempotentHint": True,
            "openWorldHint": False,
        },
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Three-part Unity Catalog or Delta table name (catalog.schema.table).",
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
        "outputSchema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Analyzed Delta table identifier.",
                },
                "total_files": {
                    "type": "integer",
                    "description": "Total number of Parquet files evaluated.",
                },
                "total_size_mb": {
                    "type": "number",
                    "description": "Total table size in megabytes.",
                },
                "small_file_count": {
                    "type": "integer",
                    "description": "Number of files smaller than the threshold.",
                },
                "small_file_ratio": {
                    "type": "number",
                    "description": "Fraction of files that are small (0.0 to 1.0).",
                },
                "compaction_urgency": {
                    "type": "string",
                    "enum": ["CRITICAL", "HIGH", "MODERATE", "LOW", "OPTIMAL"],
                    "description": "Urgency rating for running OPTIMIZE.",
                },
                "recommended_sql": {
                    "type": "string",
                    "description": "Targeted SQL remediation command to compact small files.",
                },
            },
            "required": [
                "table_name",
                "total_files",
                "small_file_ratio",
                "compaction_urgency",
                "recommended_sql",
            ],
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
