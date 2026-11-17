"""Handler for 'dlt-doctor mcp' subcommand."""

from __future__ import annotations

import argparse

from dlt_doctor.mcp_server import serve_stdio


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    subparsers.add_parser(
        "mcp",
        help="Start a Model Context Protocol (MCP) stdio server exposing dlt-doctor tools",
    )


def handle(args: argparse.Namespace) -> int:
    _ = args
    return serve_stdio()
