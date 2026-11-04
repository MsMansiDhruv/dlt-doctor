"""Automated code synthesis and diff generation for zero-loss quarantine pipelines (WP3)."""

from __future__ import annotations

import ast
import difflib
import re
from dataclasses import dataclass
from pathlib import Path

from dlt_doctor.expectations import ExpectationRule


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
    flavor = (
        "dp" if ("pyspark.pipelines" in source_code or "pipelines as dp" in source_code) else "dlt"
    )

    # Find functions with expect_or_drop
    target_funcs: list[ast.FunctionDef] = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            has_drop = any(_is_drop_decorator(dec) for dec in node.decorator_list)
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
        # Refuse to heal materialized views reading with dlt.read
        is_mv = any(_is_materialized_view(d) for d in func.decorator_list)
        if is_mv and _reads_with_dlt_read(func):
            return HealResult(
                original_code=source_code,
                healed_code=source_code,
                diff="",
                can_heal=False,
                reason=(
                    f"Refusing to auto-heal materialized view '{func.name}' reading with dlt.read; "
                    "materialized views require snapshot refresh semantics."
                ),
            )

        # Extract table decorator
        table_dec = next((d for d in func.decorator_list if _is_table_decorator(d)), None)
        table_name = func.name
        if table_dec and isinstance(table_dec, ast.Call):
            for kw in table_dec.keywords:
                if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                    table_name = str(kw.value.value)

        # Extract expectation rules from drop decorators
        rules: list[ExpectationRule] = []
        for dec in func.decorator_list:
            if _is_drop_decorator(dec) and isinstance(dec, ast.Call):
                if (
                    len(dec.args) >= 2
                    and isinstance(dec.args[0], ast.Constant)
                    and isinstance(dec.args[1], ast.Constant)
                ):
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

        # Detect streaming vs batch
        is_streaming = _detect_is_streaming(func)

        # Partition decorators
        non_drop_expect_decs = [
            d for d in func.decorator_list if _is_non_drop_expectation_decorator(d)
        ]
        custom_decs = [
            d
            for d in func.decorator_list
            if not _is_drop_decorator(d) and d != table_dec and d not in non_drop_expect_decs
        ]

        # Extract text snippets
        def_line = func.lineno
        last_func_line = getattr(func, "end_lineno", func.lineno) or func.lineno

        # Extract table decorator text
        table_dec_text = ""
        if table_dec:
            td_start = table_dec.lineno - 1
            td_end = getattr(table_dec, "end_lineno", table_dec.lineno)
            table_dec_text = "".join(lines[td_start:td_end]).strip()
        else:
            table_dec_text = "@dp.table" if flavor == "dp" else "@dlt.table"

        # Extract non-drop expectation decorator text
        expect_decs_text_list: list[str] = []
        for ed in non_drop_expect_decs:
            ed_start = ed.lineno - 1
            ed_end = getattr(ed, "end_lineno", ed.lineno)
            expect_decs_text_list.append("".join(lines[ed_start:ed_end]).strip())

        # Extract custom decorator text
        custom_decs_text_list: list[str] = []
        for cd in custom_decs:
            cd_start = cd.lineno - 1
            cd_end = getattr(cd, "end_lineno", cd.lineno)
            custom_decs_text_list.append("".join(lines[cd_start:cd_end]).strip())

        # Extract function body lines (from def to end)
        func_body_lines = lines[def_line - 1 : last_func_line]
        first_line_body = func_body_lines[0]
        renamed_def_line = re.sub(
            rf"\bdef\s+{re.escape(func.name)}\b",
            f"def {func.name}_raw",
            first_line_body,
            count=1,
        )
        raw_func_code = renamed_def_line + "".join(func_body_lines[1:])
        if not raw_func_code.endswith("\n"):
            raw_func_code += "\n"

        # Build raw view decorator
        raw_view_name = f"{table_name}_raw"
        if flavor == "dp":
            raw_dec = f'@dp.temporary_view(name="{raw_view_name}")'
        else:
            raw_dec = f'@dlt.view(name="{raw_view_name}")'

        # Build staged view
        staged_view_name = f"{table_name}_staged"
        if flavor == "dp":
            staged_dec = f'@dp.temporary_view(name="{staged_view_name}")'
            read_raw_expr = (
                f'spark.readStream.table("{raw_view_name}")'
                if is_streaming
                else f'spark.read.table("{raw_view_name}")'
            )
            read_staged_expr = (
                f'spark.readStream.table("{staged_view_name}")'
                if is_streaming
                else f'spark.read.table("{staged_view_name}")'
            )
        else:
            staged_dec = f'@dlt.view(name="{staged_view_name}")'
            read_raw_expr = (
                f'dlt.read_stream("{raw_view_name}")'
                if is_streaming
                else f'dlt.read("{raw_view_name}")'
            )
            read_staged_expr = (
                f'dlt.read_stream("{staged_view_name}")'
                if is_streaming
                else f'dlt.read("{staged_view_name}")'
            )

        # Build condition expressions for staged view
        failed_items: list[str] = []
        for r in rules:
            cond_escaped = r.condition_sql.replace('"', '\\"')
            r_escaped = r.rule_name.replace('"', '\\"')
            failed_items.append(
                f'                F.when(~F.expr("coalesce(({cond_escaped}), false)"), F.lit("{r_escaped}")),'
            )
        failed_rules_block = "\n".join(failed_items)

        staged_func_code = (
            f"{staged_dec}\n"
            f"def {func.name}_staged():\n"
            f"    return (\n"
            f"        {read_raw_expr}\n"
            f"        .select(\n"
            f'            "*",\n'
            f"            F.array_compact(\n"
            f"                F.array(\n"
            f"{failed_rules_block}\n"
            f"                )\n"
            f'            ).alias("_failed_rules"),\n'
            f'            F.current_timestamp().alias("_quarantined_at"),\n'
            f"        )\n"
            f"    )\n"
        )

        # Build clean table
        clean_decs: list[str] = [table_dec_text]
        clean_decs.extend(expect_decs_text_list)
        clean_decs_str = "\n".join(clean_decs)

        clean_func_code = (
            f"{clean_decs_str}\n"
            f"def {func.name}():\n"
            f"    return (\n"
            f"        {read_staged_expr}\n"
            f'        .filter(F.size(F.col("_failed_rules")) == 0)\n'
            f'        .drop("_failed_rules", "_quarantined_at")\n'
            f"    )\n"
        )

        # Build quarantine table
        quarantine_table_name = f"{table_name}_quarantine"
        if flavor == "dp":
            quarantine_dec = f'@dp.table(name="{quarantine_table_name}")'
        else:
            quarantine_dec = f'@dlt.table(name="{quarantine_table_name}")'

        quarantine_func_code = (
            f"{quarantine_dec}\n"
            f"def {func.name}_quarantine():\n"
            f"    return (\n"
            f"        {read_staged_expr}\n"
            f'        .filter(F.size(F.col("_failed_rules")) > 0)\n'
            f"    )\n"
        )

        # Combine all parts
        custom_decs_prefix = (
            "\n".join(custom_decs_text_list) + "\n" if custom_decs_text_list else ""
        )
        replacement_code = (
            f"{custom_decs_prefix}{raw_dec}\n"
            f"{raw_func_code}\n\n"
            f"{staged_func_code}\n"
            f"{clean_func_code}\n"
            f"{quarantine_func_code}"
        )

        # Determine line boundaries to replace
        first_dec_line = min(getattr(d, "lineno", func.lineno) for d in func.decorator_list)
        replacements.append((first_dec_line, last_func_line, replacement_code))
        healed_tables.append(table_name)

    # Apply replacements from bottom to top
    replacements.sort(key=lambda r: r[0], reverse=True)
    modified_lines = list(lines)

    for start_line, end_line, new_code in replacements:
        prefix = modified_lines[: start_line - 1]
        suffix = modified_lines[end_line:]
        snippet_with_newline = new_code if new_code.endswith("\n") else new_code + "\n"
        modified_lines = [*prefix, snippet_with_newline, *suffix]

    healed_code = "".join(modified_lines)

    # Hoist necessary imports once at the top
    healed_code = _hoist_imports_once(healed_code, flavor=flavor)

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


def _is_non_drop_expectation_decorator(dec: ast.AST) -> bool:
    name = ""
    if isinstance(dec, ast.Name):
        name = dec.id
    elif isinstance(dec, ast.Attribute):
        name = dec.attr
    elif isinstance(dec, ast.Call):
        return _is_non_drop_expectation_decorator(dec.func)
    return name in {"expect", "expect_or_fail", "expect_all", "expect_all_or_fail"}


def _is_table_decorator(dec: ast.AST) -> bool:
    name = ""
    if isinstance(dec, ast.Name):
        name = dec.id
    elif isinstance(dec, ast.Attribute):
        name = dec.attr
    elif isinstance(dec, ast.Call):
        return _is_table_decorator(dec.func)
    return name in {"table", "view", "materialized_view", "streaming_table"}


def _is_materialized_view(dec: ast.AST) -> bool:
    name = ""
    if isinstance(dec, ast.Name):
        name = dec.id
    elif isinstance(dec, ast.Attribute):
        name = dec.attr
    elif isinstance(dec, ast.Call):
        return _is_materialized_view(dec.func)
    return name == "materialized_view"


def _reads_with_dlt_read(func_node: ast.FunctionDef) -> bool:
    for node in ast.walk(func_node):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "read"
        ):
            caller = getattr(node.func.value, "id", "")
            if caller in {"dlt", "dp"}:
                return True
    return False


def _detect_is_streaming(func_node: ast.FunctionDef) -> bool:
    for node in ast.walk(func_node):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"readStream", "read_stream"}
        ):
            return True
        if isinstance(node, ast.Attribute) and node.attr == "readStream":
            return True
    return False


def _hoist_imports_once(code: str, flavor: str = "dlt") -> str:
    """Ensure required imports are hoisted once at top of code."""
    needed_imports: list[str] = []
    if "from pyspark.sql import functions as F" not in code:
        needed_imports.append("from pyspark.sql import functions as F")
    if flavor == "dlt" and not re.search(r"\bimport\s+dlt\b", code):
        needed_imports.append("import dlt")
    elif flavor == "dp" and not re.search(r"\bpipelines\s+as\s+dp\b", code):
        needed_imports.append("from pyspark import pipelines as dp")

    if not needed_imports:
        return code

    header = "\n".join(needed_imports) + "\n\n"
    return header + code
