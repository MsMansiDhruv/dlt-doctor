"""Databricks and Jupyter notebook parsing engine for PySpark AST analysis."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ParsedCell:
    """An individual code cell extracted from a notebook."""

    cell_index: int
    cell_type: str  # "python", "sql", "skipped"
    source_code: str
    original_line_offset: int = 1


def is_notebook_file(path: Path | str) -> bool:
    """Check if the path represents a Jupyter (.ipynb) or Databricks source (.py) notebook."""
    p = Path(path)
    if p.suffix.lower() == ".ipynb":
        return True
    if p.suffix.lower() == ".py" and p.is_file():
        try:
            head = p.read_text(encoding="utf-8", errors="ignore")[:300]
            if "# Databricks notebook source" in head:
                return True
        except (OSError, UnicodeDecodeError):
            return False
    return False


def parse_notebook_cells(path: Path | str) -> list[ParsedCell]:
    """Parse notebook into distinct, individually analyzable code cells."""
    notebook_path = Path(path)
    if not notebook_path.exists():
        raise FileNotFoundError(f"Notebook not found: {notebook_path}")

    if notebook_path.suffix.lower() == ".ipynb":
        return _parse_ipynb_cells(notebook_path)
    return _parse_databricks_py_cells(notebook_path)


def _parse_ipynb_cells(path: Path) -> list[ParsedCell]:
    try:
        content = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in notebook: {path}") from exc

    parsed_cells: list[ParsedCell] = []
    cells = content.get("cells", [])

    for idx, cell in enumerate(cells, start=1):
        if cell.get("cell_type") != "code":
            continue

        raw_source = cell.get("source", [])
        if isinstance(raw_source, str):
            lines = raw_source.splitlines(keepends=True)
        else:
            lines = list(raw_source)

        if not lines:
            continue

        first_line = lines[0].strip()

        # Handle cell magics: skip entire non-code cell
        if first_line.startswith(("%%sql", "%sql")):
            # Extract SQL body
            sql_lines = lines[1:] if first_line in {"%%sql", "%sql"} else [first_line[4:].strip() + "\n", *lines[1:]]
            parsed_cells.append(
                ParsedCell(
                    cell_index=idx,
                    cell_type="sql",
                    source_code="".join(sql_lines),
                )
            )
        elif first_line.startswith(("%md", "%%sh", "%%bash", "%%html", "%%javascript", "%fs")):
            parsed_cells.append(
                ParsedCell(
                    cell_index=idx,
                    cell_type="skipped",
                    source_code="".join(lines),
                )
            )
        else:
            # Python code cell: filter out line magics (e.g. %load_ext, %pip, !pip, %run)
            sanitized: list[str] = []
            for line in lines:
                stripped = line.strip()
                if stripped.startswith(("%", "!")):
                    sanitized.append(f"# [magic skipped] {stripped}\n")
                else:
                    sanitized.append(line if line.endswith("\n") else line + "\n")

            parsed_cells.append(
                ParsedCell(
                    cell_index=idx,
                    cell_type="python",
                    source_code="".join(sanitized),
                )
            )

    return parsed_cells


def _parse_databricks_py_cells(path: Path) -> list[ParsedCell]:
    text = path.read_text(encoding="utf-8")
    raw_blocks = text.split("# COMMAND ----------")
    parsed_cells: list[ParsedCell] = []

    for idx, block in enumerate(raw_blocks, start=1):
        lines = block.splitlines(keepends=True)
        non_empty = [line for line in lines if line.strip() and not line.strip().startswith("# Databricks notebook source")]
        if not non_empty:
            continue

        first_line = non_empty[0].strip()

        if "# MAGIC %sql" in first_line:
            # Extract SQL lines, stripping # MAGIC
            sql_lines = []
            for line in non_empty:
                stripped = line.strip()
                if stripped.startswith("# MAGIC %sql"):
                    rest = stripped.replace("# MAGIC %sql", "").strip()
                    if rest:
                        sql_lines.append(rest + "\n")
                elif stripped.startswith("# MAGIC"):
                    sql_lines.append(stripped.replace("# MAGIC", "", 1).strip() + "\n")
            parsed_cells.append(
                ParsedCell(
                    cell_index=idx,
                    cell_type="sql",
                    source_code="".join(sql_lines),
                )
            )
        elif any(magic in first_line for magic in ["# MAGIC %md", "# MAGIC %sh", "# MAGIC %run", "# MAGIC %pip"]):
            parsed_cells.append(
                ParsedCell(
                    cell_index=idx,
                    cell_type="skipped",
                    source_code=block,
                )
            )
        else:
            # Python cell
            clean_lines = []
            for line in lines:
                if line.strip().startswith("# Databricks notebook source"):
                    continue
                clean_lines.append(line if line.endswith("\n") else line + "\n")
            parsed_cells.append(
                ParsedCell(
                    cell_index=idx,
                    cell_type="python",
                    source_code="".join(clean_lines),
                )
            )

    return parsed_cells


def extract_notebook_code(path: Path | str) -> str:
    """Backward-compatible helper extracting combined Python code from notebook."""
    cells = parse_notebook_cells(path)
    return "\n\n".join(c.source_code for c in cells if c.cell_type == "python")
