"""Automated code synthesis and diff generation for zero-loss quarantine pipelines (WP3)."""

from __future__ import annotations

import ast
import difflib
from dataclasses import dataclass
from pathlib import Path

from dlt_doctor.expectations import ExpectationDoctor, ExpectationRule


@dataclass
class HealResult:
    """Result of auto-healing a source file or snippet."""

    original_code: str
    healed_code: str
    diff: str
    can_heal: bool
    reason: str = ""
    tables_healed: list[str] | None = None


def auto_heal_source(source_code: str, file_path: str = "pipeline.py") -> HealResult:
    """Analyze source code and synthesize single-pass quarantine tables for expect_or_drop."""
    try:
        tree = ast.parse(source_code)
    except SyntaxError as exc:
        return HealResult(
            original_code=source_code,
            healed_code=source_code,
            diff="",
            can_heal=False,
            reason=f"Syntax error prevents auto-healing: {exc}",
        )

    # Detect flavor (dp vs dlt)
    flavor = "dp" if ("pyspark.pipelines" in source_code or "pipelines as dp" in source_code) else "dlt"

    # Find functions with expect_or_drop
    target_funcs: list[ast.FunctionDef] = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            has_drop = any(
                _is_drop_decorator(dec) for dec in node.decorator_list
            )
            if has_drop:
                target_funcs.append(node)

    if not target_funcs:
        return HealResult(
            original_code=source_code,
            healed_code=source_code,
            diff="",
            can_heal=True,
            reason="No expect_or_drop decorators found; file is already compliant.",
            tables_healed=[],
        )

    lines = source_code.splitlines(keepends=True)
    replacements: list[tuple[int, int, str]] = []  # (start_line_1_idx, end_line_1_idx, new_code)
    healed_tables: list[str] = []

    for func in target_funcs:
        # Check shape: body must be single return statement (or docstring + return)
        body = [stmt for stmt in func.body if not (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant))]
        if len(body) != 1 or not isinstance(body[0], ast.Return):
            return HealResult(
                original_code=source_code,
                healed_code=source_code,
                diff="",
                can_heal=False,
                reason=(
                    f"Cannot auto-heal function '{func.name}': multi-statement bodies "
                    "require manual refactoring to prevent side-effect regressions."
                ),
            )

        # Extract table decorator metadata
        table_name = func.name
        for dec in func.decorator_list:
            if _is_table_decorator(dec) and isinstance(dec, ast.Call):
                for kw in dec.keywords:
                    if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                        table_name = str(kw.value.value)
                    elif kw.arg == "comment" and isinstance(kw.value, ast.Constant):
                        str(kw.value.value)

        # Extract source table from return expression
        source_table = _extract_source_table(body[0].value) or "source_bronze"

        # Extract expectation rules
        rules: list[ExpectationRule] = []
        for dec in func.decorator_list:
            if _is_drop_decorator(dec) and isinstance(dec, ast.Call):
                if len(dec.args) >= 2 and isinstance(dec.args[0], ast.Constant) and isinstance(dec.args[1], ast.Constant):
                    rule_name = str(dec.args[0].value)
                    condition_sql = str(dec.args[1].value)
                    rules.append(ExpectationRule(rule_name=rule_name, condition_sql=condition_sql))
                elif len(dec.args) == 1 and isinstance(dec.args[0], ast.Dict):
                    for k, v in zip(dec.args[0].keys, dec.args[0].values, strict=False):
                        if (
                            isinstance(k, ast.Constant)
                            and isinstance(k.value, str)
                            and isinstance(v, ast.Constant)
                            and isinstance(v.value, str)
                        ):
                            rules.append(ExpectationRule(rule_name=k.value, condition_sql=v.value))
                else:
                    return HealResult(
                        original_code=source_code,
                        healed_code=source_code,
                        diff="",
                        can_heal=False,
                        reason=(
                            f"Cannot auto-heal function '{func.name}': dynamic expectation "
                            "variables cannot be safely resolved statically."
                        ),
                    )

        if not rules:
            continue

        # Build replacement code using ExpectationDoctor single-pass routing
        doctor = ExpectationDoctor()
        plan = doctor.build_quarantine_routing(table_name, rules)
        generated_snippet = doctor.generate_pipeline_code(plan, source_table=source_table, flavor=flavor)

        # Determine line boundaries to replace
        first_dec_line = min(getattr(d, "lineno", func.lineno) for d in func.decorator_list)
        last_func_line = getattr(func, "end_lineno", func.lineno) or func.lineno
        replacements.append((first_dec_line, last_func_line, generated_snippet))
        healed_tables.append(table_name)

    # Apply replacements from bottom to top so line numbers don't shift
    replacements.sort(key=lambda r: r[0], reverse=True)
    modified_lines = list(lines)

    for start_line, end_line, new_code in replacements:
        # start_line and end_line are 1-indexed
        prefix = modified_lines[: start_line - 1]
        suffix = modified_lines[end_line:]
        snippet_with_newline = new_code if new_code.endswith("\n") else new_code + "\n"
        modified_lines = [*prefix, snippet_with_newline, *suffix]

    healed_code = "".join(modified_lines)

    # Verify generated Python code parses
    try:
        ast.parse(healed_code)
    except SyntaxError as exc:
        return HealResult(
            original_code=source_code,
            healed_code=source_code,
            diff="",
            can_heal=False,
            reason=f"Synthesized code failed validation check: {exc}",
        )

    # Generate unified diff
    orig_split = source_code.splitlines(keepends=True)
    healed_split = healed_code.splitlines(keepends=True)
    diff = "".join(
        difflib.unified_diff(
            orig_split,
            healed_split,
            fromfile=f"a/{file_path}",
            tofile=f"b/{file_path}",
        )
    )

    return HealResult(
        original_code=source_code,
        healed_code=healed_code,
        diff=diff,
        can_heal=True,
        tables_healed=healed_tables,
    )


def heal_file(file_path: Path | str, write: bool = False) -> HealResult:
    """Run auto-heal on a file path, optionally writing back the changes."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    source_code = path.read_text(encoding="utf-8")
    result = auto_heal_source(source_code, file_path=path.name)

    if write and result.can_heal and result.diff:
        path.write_text(result.healed_code, encoding="utf-8")

    return result


def _is_drop_decorator(dec: ast.AST) -> bool:
    name = ""
    if isinstance(dec, ast.Name):
        name = dec.id
    elif isinstance(dec, ast.Attribute):
        name = dec.attr
    elif isinstance(dec, ast.Call):
        return _is_drop_decorator(dec.func)
    return name in {"expect_or_drop", "expect_all_or_drop"}


def _is_table_decorator(dec: ast.AST) -> bool:
    name = ""
    if isinstance(dec, ast.Name):
        name = dec.id
    elif isinstance(dec, ast.Attribute):
        name = dec.attr
    elif isinstance(dec, ast.Call):
        return _is_table_decorator(dec.func)
    return name in {"table", "view", "materialized_view", "streaming_table"}


def _extract_source_table(expr: ast.AST | None) -> str | None:
    if expr is None:
        return None
    for node in ast.walk(expr):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"read_stream", "read", "table"}
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            return node.args[0].value
    return None
