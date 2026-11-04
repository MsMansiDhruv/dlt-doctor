"""Databricks and Jupyter notebook parsing engine for PySpark AST analysis."""

from __future__ import annotations

import json
from pathlib import Path


def is_notebook_file(path: Path | str) -> bool:
    """Check if the path represents a Jupyter or Databricks notebook."""
    return Path(path).suffix.lower() == ".ipynb"


def extract_notebook_code(path: Path | str) -> str:
    """Extract and sanitize Python code from a Jupyter or Databricks notebook.

    Preserves line structure so AST line numbers correspond to the notebook source.
    IPython and Databricks magic lines (e.g. %sql, %sh, %pip, !pip) are converted
    to inert Python comments so AST parsing succeeds.
    """
    notebook_path = Path(path)
    if not notebook_path.exists():
        raise FileNotFoundError(f"Notebook not found: {notebook_path}")

    try:
        content = json.loads(notebook_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in notebook: {notebook_path}") from exc

    extracted_lines: list[str] = []
    cells = content.get("cells", [])

    for cell_idx, cell in enumerate(cells, start=1):
        if cell.get("cell_type") != "code":
            continue

        raw_source = cell.get("source", [])
        if isinstance(raw_source, str):
            lines = raw_source.splitlines(keepends=True)
        else:
            lines = list(raw_source)

        extracted_lines.append(f"# [dlt-doctor] Notebook Cell {cell_idx}\n")

        for line in lines:
            stripped = line.strip()
            # Handle Databricks / IPython cell or line magics
            if stripped.startswith("%") or stripped.startswith("!"):
                # Replace with comment of identical line count
                extracted_lines.append(f"# [magic skipped] {stripped}\n")
            else:
                extracted_lines.append(line if line.endswith("\n") else line + "\n")

        extracted_lines.append("\n")

    return "".join(extracted_lines)
