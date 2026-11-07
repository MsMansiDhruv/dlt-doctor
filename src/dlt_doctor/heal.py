"""Automated code synthesis and diff generation for zero-loss quarantine pipelines (WP3)."""

from __future__ import annotations

import ast
import difflib
import re
from dataclasses import dataclass
from pathlib import Path

import sqlglot
from sqlglot import expressions as exp

from dlt_doctor.expectations import (
    ExpectationRule,
    generate_healed_pipeline_nodes,
)


@dataclass
class HealResult:
    """Result of auto-healing a source file or snippet."""

    original_code: str
    healed_code: str
    diff: str
    can_heal: bool
    reason: str = ""
    tables_healed: list[str] | None = None


SUFFIX_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
NONDETERMINISTIC_FUNCS = {
    "rand",
    "randn",
    "uuid",
    "random",
}
STATEFUL_STREAMING_METHODS = {
    "groupby",
    "group_by",
    "agg",
    "aggregate",
    "dropduplicates",
    "drop_duplicates",
    "withwatermark",
    "with_watermark",
}
SQL_NONDETERMINISTIC_FUNCS = {
    "current_timestamp",
    "current_date",
    "now",
    "rand",
    "randn",
    "uuid",
    "monotonically_increasing_id",
    "unix_timestamp",
    "current_timezone",
}


def _is_str_join(node: ast.Call) -> bool:
    """Check if an ast.Call is a string join call (e.g. ', '.join(cols))."""
    if not (isinstance(node.func, ast.Attribute) and node.func.attr.lower() == "join"):
        return False
    # Receiver is a string literal (e.g. ", ".join(...))
    if isinstance(node.func.value, ast.Constant) and isinstance(node.func.value.value, str):
        return True
    # Receiver is an f-string or string formatting
    if isinstance(node.func.value, ast.JoinedStr):
        return True
    # Single argument is a list/tuple/set literal, e.g. sep.join(['a', 'b'])
    return len(node.args) == 1 and isinstance(node.args[0], (ast.List, ast.Tuple, ast.Set))


def _count_stream_reads(func_node: ast.FunctionDef) -> int:
    count = 0
    for node in ast.walk(func_node):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"readStream", "read_stream"}
        ):
            count += 1
    return count


def _has_stateful_or_nondet_calls(
    func_node: ast.FunctionDef, is_streaming: bool
) -> tuple[bool, str]:
    num_streams = _count_stream_reads(func_node) if is_streaming else 0

    for node in ast.walk(func_node):
        if isinstance(node, ast.Call):
            func_name = ""
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                func_name = node.func.attr
            name_lower = func_name.lower()
            if name_lower in NONDETERMINISTIC_FUNCS:
                return True, f"non-deterministic call '{func_name}'"
            if is_streaming:
                if name_lower in STATEFUL_STREAMING_METHODS:
                    return True, f"stateful streaming operation '{func_name}'"
                if name_lower == "join":
                    if _is_str_join(node):
                        continue
                    # Stream-stream joins (2+ streams) are stateful; stream-static joins (1 stream) are allowed
                    if num_streams > 1:
                        return True, f"stateful stream-stream join '{func_name}'"
    return False, ""


def _has_nondeterministic_sql(condition_sql: str) -> str | None:
    try:
        parsed = sqlglot.parse(condition_sql, read="spark")
    except Exception:
        return None
    for stmt in parsed:
        if stmt is None:
            continue
        for node in stmt.walk():
            name = ""
            if isinstance(node, exp.Func):
                name = node.sql_name().lower()
            elif isinstance(node, exp.Anonymous):
                name = node.this.lower()
            if name in SQL_NONDETERMINISTIC_FUNCS:
                return name
    return None


def auto_heal_source(
    source_code: str, file_path: str = "pipeline.py", quarantine_suffix: str = "_quarantine"
) -> HealResult:
    """Analyze source code and synthesize single-pass quarantine tables for expect_or_drop."""
    if not SUFFIX_PATTERN.match(quarantine_suffix):
        raise ValueError(
            f"Invalid quarantine_suffix '{quarantine_suffix}'. Must match ^[A-Za-z0-9_]+$."
        )
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

    # Check for any nested function definitions with expect_or_drop
    all_funcs = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
    top_level_funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
    nested_drop_funcs = [
        f
        for f in all_funcs
        if f not in top_level_funcs and any(_is_drop_decorator(dec) for dec in f.decorator_list)
    ]
    if nested_drop_funcs:
        first_nested = nested_drop_funcs[0]
        return HealResult(
            original_code=source_code,
            healed_code=source_code,
            diff="",
            can_heal=False,
            reason=(
                f"Refusing to auto-heal nested function '{first_nested.name}': functions defined "
                "inside loops, factories, or classes cannot be safely rewritten at module scope."
            ),
        )

    # Collect existing top-level defined names to guard against collisions
    existing_top_level_names: set[str] = set()
    for stmt in tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.ClassDef)):
            existing_top_level_names.add(stmt.name)
        elif isinstance(stmt, ast.Assign):
            for tgt in stmt.targets:
                if isinstance(tgt, ast.Name):
                    existing_top_level_names.add(tgt.id)

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

        # Detect streaming vs batch
        is_streaming = _detect_is_streaming(func)

        is_bad, bad_reason = _has_stateful_or_nondet_calls(func, is_streaming=is_streaming)
        if is_bad:
            return HealResult(
                original_code=source_code,
                healed_code=source_code,
                diff="",
                can_heal=False,
                reason=(
                    f"Refusing to auto-heal function '{func.name}': function body contains "
                    f"{bad_reason} that would recompute different values across stages."
                ),
            )

        # Extract table decorator and table name (support both positional and keyword 'name')
        table_dec = next((d for d in func.decorator_list if _is_table_decorator(d)), None)
        table_name = func.name
        if table_dec and isinstance(table_dec, ast.Call):
            if table_dec.args:
                if isinstance(table_dec.args[0], ast.Constant) and isinstance(
                    table_dec.args[0].value, str
                ):
                    table_name = str(table_dec.args[0].value)
                else:
                    return HealResult(
                        original_code=source_code,
                        healed_code=source_code,
                        diff="",
                        can_heal=False,
                        reason=(
                            f"Cannot auto-heal function '{func.name}': dynamic or non-constant "
                            "table name decorator argument cannot be safely resolved statically."
                        ),
                    )
            for kw in table_dec.keywords:
                if kw.arg == "name":
                    if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                        table_name = str(kw.value.value)
                    else:
                        return HealResult(
                            original_code=source_code,
                            healed_code=source_code,
                            diff="",
                            can_heal=False,
                            reason=(
                                f"Cannot auto-heal function '{func.name}': dynamic or non-constant "
                                "table name keyword argument cannot be safely resolved statically."
                            ),
                        )

        # Extract expectation rules from drop decorators
        rules: list[ExpectationRule] = []
        for dec in func.decorator_list:
            if _is_drop_decorator(dec) and isinstance(dec, ast.Call):
                if (
                    len(dec.args) >= 2
                    and isinstance(dec.args[0], ast.Constant)
                    and isinstance(dec.args[0].value, str)
                    and isinstance(dec.args[1], ast.Constant)
                    and isinstance(dec.args[1].value, str)
                ):
                    rule_name = str(dec.args[0].value)
                    condition_sql = str(dec.args[1].value)
                    try:
                        rule = ExpectationRule(rule_name=rule_name, condition_sql=condition_sql)
                    except ValueError as exc:
                        return HealResult(
                            original_code=source_code,
                            healed_code=source_code,
                            diff="",
                            can_heal=False,
                            reason=f"Cannot auto-heal function '{func.name}': invalid expectation rule '{rule_name}': {exc}",
                        )
                    nondet_sql = _has_nondeterministic_sql(condition_sql)
                    if nondet_sql:
                        return HealResult(
                            original_code=source_code,
                            healed_code=source_code,
                            diff="",
                            can_heal=False,
                            reason=(
                                f"Refusing to auto-heal function '{func.name}': expectation rule '{rule_name}' "
                                f"contains non-deterministic SQL function '{nondet_sql}'."
                            ),
                        )
                    rules.append(rule)
                elif len(dec.args) == 1 and isinstance(dec.args[0], ast.Dict):
                    for k, v in zip(dec.args[0].keys, dec.args[0].values, strict=False):
                        if (
                            k is None
                            or not isinstance(k, ast.Constant)
                            or not isinstance(k.value, str)
                            or not isinstance(v, ast.Constant)
                            or not isinstance(v.value, str)
                        ):
                            return HealResult(
                                original_code=source_code,
                                healed_code=source_code,
                                diff="",
                                can_heal=False,
                                reason=(
                                    f"Cannot auto-heal function '{func.name}': dynamic expectation "
                                    "dictionary keys, values, or **spread cannot be safely resolved statically."
                                ),
                            )
                        try:
                            rule = ExpectationRule(rule_name=k.value, condition_sql=v.value)
                        except ValueError as exc:
                            return HealResult(
                                original_code=source_code,
                                healed_code=source_code,
                                diff="",
                                can_heal=False,
                                reason=f"Cannot auto-heal function '{func.name}': invalid expectation rule '{k.value}': {exc}",
                            )
                        nondet_sql = _has_nondeterministic_sql(v.value)
                        if nondet_sql:
                            return HealResult(
                                original_code=source_code,
                                healed_code=source_code,
                                diff="",
                                can_heal=False,
                                reason=(
                                    f"Refusing to auto-heal function '{func.name}': expectation rule '{k.value}' "
                                    f"contains non-deterministic SQL function '{nondet_sql}'."
                                ),
                            )
                        rules.append(rule)
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

        # Compute leaf names for session views to avoid multi-part temporary view name errors
        leaf_name = table_name.split(".")[-1]
        table_prefix = table_name[: -len(leaf_name)]
        raw_view_name = f"{leaf_name}_raw"
        staged_view_name = f"{leaf_name}_staged"
        quarantine_table_name = f"{table_prefix}{leaf_name}{quarantine_suffix}"

        # Refuse on name collisions with existing top-level definitions
        generated_names = {
            f"{func.name}_raw",
            f"{func.name}_staged",
            f"{func.name}_quarantine",
            raw_view_name,
            staged_view_name,
            quarantine_table_name,
        }
        for cand in generated_names:
            if cand in existing_top_level_names and cand != func.name:
                return HealResult(
                    original_code=source_code,
                    healed_code=source_code,
                    diff="",
                    can_heal=False,
                    reason=(
                        f"Refusing to auto-heal function '{func.name}': synthesized name "
                        f"'{cand}' collides with an existing definition in the file."
                    ),
                )

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
        if flavor == "dp":
            raw_dec = f'@dp.temporary_view(name="{raw_view_name}")'
        else:
            raw_dec = f'@dlt.view(name="{raw_view_name}")'

        # Extract table_properties from table_dec if present
        extracted_table_props: dict[str, str] = {}
        if isinstance(table_dec, ast.Call):
            for kw in table_dec.keywords:
                if kw.arg == "table_properties" and isinstance(kw.value, ast.Dict):
                    for k, v in zip(kw.value.keys, kw.value.values, strict=False):
                        if (
                            isinstance(k, ast.Constant)
                            and isinstance(v, ast.Constant)
                            and isinstance(k.value, str)
                            and isinstance(v.value, str)
                        ):
                            extracted_table_props[k.value] = v.value

        staged_func_code, clean_func_code, quarantine_func_code = generate_healed_pipeline_nodes(
            clean_func_name=func.name,
            clean_table_name=table_name,
            quarantine_table_name=quarantine_table_name,
            source_table_or_view=raw_view_name,
            rules=rules,
            flavor=flavor,
            is_streaming=is_streaming,
            clean_table_decorator_text=table_dec_text,
            clean_extra_decorators=expect_decs_text_list,
            table_properties=extracted_table_props,
            quarantine_func_name=f"{func.name}_quarantine",
            staging_func_name=f"{func.name}_staged",
            staging_view_name=staged_view_name,
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


def heal_file(
    file_path: Path | str, write: bool = False, quarantine_suffix: str = "_quarantine"
) -> HealResult:
    """Run auto-heal on a file path, optionally writing back the changes."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    source_code = path.read_text(encoding="utf-8")
    result = auto_heal_source(source_code, file_path=path.name, quarantine_suffix=quarantine_suffix)

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
    """Ensure required imports are hoisted once below file headers/future imports."""
    needed_imports: list[str] = []
    if "from pyspark.sql import functions as F" not in code:
        needed_imports.append("from pyspark.sql import functions as F")
    if flavor == "dlt" and not re.search(r"\bimport\s+dlt\b", code):
        needed_imports.append("import dlt")
    elif flavor == "dp" and not re.search(r"\bpipelines\s+as\s+dp\b", code):
        needed_imports.append("from pyspark import pipelines as dp")

    if not needed_imports:
        return code

    lines = code.splitlines(keepends=True)
    insert_idx = 0

    # Skip initial notebook comment e.g. # Databricks notebook source
    while insert_idx < len(lines) and lines[insert_idx].strip().startswith("#"):
        insert_idx += 1

    # Skip blank lines before docstring
    while insert_idx < len(lines) and not lines[insert_idx].strip():
        insert_idx += 1

    # Skip module docstring
    if insert_idx < len(lines) and (
        lines[insert_idx].strip().startswith('"""') or lines[insert_idx].strip().startswith("'''")
    ):
        quote = lines[insert_idx].strip()[:3]
        if lines[insert_idx].strip().count(quote) >= 2 and len(lines[insert_idx].strip()) > 3:
            insert_idx += 1
        else:
            insert_idx += 1
            while insert_idx < len(lines) and quote not in lines[insert_idx]:
                insert_idx += 1
            if insert_idx < len(lines):
                insert_idx += 1

    # Skip future imports and blank lines
    while insert_idx < len(lines):
        line = lines[insert_idx].strip()
        if not line or line.startswith("#") or line.startswith("from __future__"):
            insert_idx += 1
        else:
            break

    import_block = "\n".join(needed_imports) + "\n\n"
    prefix = "".join(lines[:insert_idx])
    if prefix and not prefix.endswith("\n"):
        prefix += "\n"
    suffix = "".join(lines[insert_idx:])
    return prefix + import_block + suffix
