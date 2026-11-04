"""AST visitor and SQL scanner for Lakeflow / DLT expectation rules (DD001, DD002, DD003)."""

from __future__ import annotations

import ast
import re

import sqlglot
from sqlglot.expressions import Expr
from sqlglot.optimizer.simplify import simplify

from dlt_doctor.catalyst import DiagnosticFinding, DiagnosticSeverity
from dlt_doctor.config import DoctorConfig

NONDETERMINISTIC_FUNCS = {
    "current_timestamp",
    "now",
    "current_date",
    "today",
    "rand",
    "randn",
    "random",
    "uuid",
    "monotonically_increasing_id",
    "spark_partition_id",
}


def _find_nondeterministic_function(cond_sql: str) -> str | None:
    try:
        parsed = sqlglot.parse_one(cond_sql, read="spark")
    except sqlglot.errors.SqlglotError:
        return None

    for node in parsed.walk():
        if isinstance(
            node,
            (
                sqlglot.exp.CurrentTimestamp,
                sqlglot.exp.CurrentDate,
                sqlglot.exp.Rand,
                sqlglot.exp.Uuid,
            ),
        ):
            return node.key.lower()
        if isinstance(node, (sqlglot.exp.Func, sqlglot.exp.Anonymous)):
            fname = (node.name or node.sql_name() or "").lower()
            if fname in NONDETERMINISTIC_FUNCS:
                return fname
        if isinstance(node, sqlglot.exp.Column):
            cname = (node.name or "").lower()
            if cname in {"current_timestamp", "current_date"}:
                return cname
    return None


class ExpectationAstVisitor(ast.NodeVisitor):
    """Audits PySpark / Lakeflow AST for expectation anti-patterns."""

    def __init__(self, source_code: str, config: DoctorConfig | None = None) -> None:
        self.source_code = source_code
        self.config = config or DoctorConfig()
        self.findings: list[DiagnosticFinding] = []
        self.module_dicts: dict[str, dict[str, str]] = {}
        self.source_lines = source_code.splitlines()
        self.all_expectation_conditions: list[tuple[str, str]] = []
        self.filter_calls: list[tuple[ast.Call, str]] = []
        self.current_table_name: str = ""

    def visit_Assign(self, node: ast.Assign) -> None:
        """Track module-level constant dictionary assignments."""
        if isinstance(node.value, ast.Dict):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    resolved: dict[str, str] = {}
                    for k, v in zip(node.value.keys, node.value.values, strict=False):
                        if (
                            isinstance(k, ast.Constant)
                            and isinstance(k.value, str)
                            and isinstance(v, ast.Constant)
                            and isinstance(v.value, str)
                        ):
                            resolved[k.value] = v.value
                    if resolved:
                        self.module_dicts[target.id] = resolved
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Inspect pipeline function definitions and their expectation decorators."""
        table_name = node.name

        # Extract table name from table decorator if present
        for dec in node.decorator_list:
            dec_name = self._get_decorator_name(dec)
            if dec_name in {
                "table",
                "view",
                "materialized_view",
                "streaming_table",
                "temporary_view",
            } and isinstance(dec, ast.Call):
                for kw in dec.keywords:
                    if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                        table_name = str(kw.value.value)

        # Check if table is marked regulated via comment or config
        has_regulated_comment = self._has_comment_in_scope(node, "# dlt-doctor: regulated")
        is_regulated = self.config.is_regulated_table(table_name) or has_regulated_comment

        expectation_conditions: list[tuple[str, str]] = []  # (rule_name, condition_sql)

        for dec in node.decorator_list:
            dec_name = self._get_decorator_name(dec)
            lineno = getattr(dec, "lineno", node.lineno)

            if dec_name in {"expect", "expect_or_drop", "expect_or_fail"}:
                if isinstance(dec, ast.Call):
                    rule_name = "expectation"
                    cond_sql = ""
                    if len(dec.args) >= 2:
                        rule_arg = dec.args[0]
                        cond_arg = dec.args[1]
                        rule_name = (
                            str(rule_arg.value)
                            if isinstance(rule_arg, ast.Constant)
                            else "expectation"
                        )
                        if isinstance(cond_arg, ast.Constant) and isinstance(cond_arg.value, str):
                            cond_sql = cond_arg.value
                            expectation_conditions.append((rule_name, cond_sql))
                            nd_fn = _find_nondeterministic_function(cond_sql)
                            if nd_fn:
                                self._emit_dd004(node, nd_fn, cond_sql, lineno)
                        else:
                            var_repr = getattr(cond_arg, "id", None) or ast.unparse(cond_arg)
                            self._emit_dd003(node, var_repr, lineno)
                    elif len(dec.args) == 1:
                        arg = dec.args[0]
                        var_repr = getattr(arg, "id", None) or ast.unparse(arg)
                        self._emit_dd003(node, var_repr, lineno)

                    if dec_name == "expect_or_drop":
                        self._emit_dd001(
                            node,
                            table_name,
                            is_regulated,
                            rule_name if len(dec.args) >= 2 else "expect_or_drop",
                            lineno,
                        )

            elif (
                dec_name in {"expect_all", "expect_all_or_drop", "expect_all_or_fail"}
                and isinstance(dec, ast.Call)
                and dec.args
            ):
                arg = dec.args[0]
                resolved_dict: dict[str, str] = {}
                if isinstance(arg, ast.Dict):
                    for k, v in zip(arg.keys, arg.values, strict=False):
                        if (
                            isinstance(k, ast.Constant)
                            and isinstance(k.value, str)
                            and isinstance(v, ast.Constant)
                            and isinstance(v.value, str)
                        ):
                            resolved_dict[k.value] = v.value
                        else:
                            var_repr = getattr(v, "id", None) or ast.unparse(v)
                            self._emit_dd003(node, var_repr, lineno)
                elif isinstance(arg, ast.Name) and arg.id in self.module_dicts:
                    resolved_dict = self.module_dicts[arg.id]
                else:
                    var_name = getattr(arg, "id", "dynamic_expression")
                    self._emit_dd003(node, var_name, lineno)

                for r_name, c_sql in resolved_dict.items():
                    expectation_conditions.append((r_name, c_sql))
                    nd_fn = _find_nondeterministic_function(c_sql)
                    if nd_fn:
                        self._emit_dd004(node, nd_fn, c_sql, lineno)

                if dec_name == "expect_all_or_drop":
                    self._emit_dd001(node, table_name, is_regulated, "expect_all_or_drop", lineno)

        if expectation_conditions:
            self.all_expectation_conditions.extend(expectation_conditions)

        prev_table = self.current_table_name
        self.current_table_name = table_name
        self.generic_visit(node)
        self.current_table_name = prev_table

    def visit_Call(self, node: ast.Call) -> None:
        """Collect filter calls across the module."""
        if isinstance(node.func, ast.Attribute) and node.func.attr in {"filter", "where"}:
            self.filter_calls.append((node, self.current_table_name))
        self.generic_visit(node)

    def finalize(self) -> None:
        """Run post-walk checks across all collected module elements."""
        if self.all_expectation_conditions and self.filter_calls:
            self._audit_quarantine_filters_global()

    def _get_decorator_name(self, dec: ast.AST) -> str:
        """Extract simplified name of decorator."""
        if isinstance(dec, ast.Name):
            return dec.id
        if isinstance(dec, ast.Attribute):
            return dec.attr
        if isinstance(dec, ast.Call):
            return self._get_decorator_name(dec.func)
        return ""

    def _has_comment_in_scope(self, node: ast.FunctionDef, comment: str) -> bool:
        """Check if preceding lines or function body contains a specific comment."""
        first_line = node.lineno
        if node.decorator_list:
            first_line = min(getattr(d, "lineno", node.lineno) for d in node.decorator_list)
        start_line = max(0, first_line - 4)
        end_line = min(
            len(self.source_lines), getattr(node, "end_lineno", node.lineno) or node.lineno
        )
        return any(comment in self.source_lines[i] for i in range(start_line, end_line))

    def _emit_dd001(
        self,
        node: ast.FunctionDef,
        table_name: str,
        is_regulated: bool,
        rule_name: str,
        lineno: int,
    ) -> None:
        """Emit DD001 for expect_or_drop."""
        default_sev = DiagnosticSeverity.HIGH if is_regulated else DiagnosticSeverity.MEDIUM
        sev = self.config.get_severity("DD001", default_sev)

        self.findings.append(
            DiagnosticFinding(
                rule_id="DD001",
                severity=sev,
                title="Silent data loss from expect_or_drop policy",
                impact_summary=(
                    f"Function '{node.name}' uses expect_or_drop on table '{table_name}'. "
                    "Dropped rows' contents are not persisted anywhere in the pipeline "
                    "(only aggregate counts are logged). In regulated or audit-sensitive tables, "
                    "this causes irrecoverable data loss."
                ),
                recommendation=(
                    "Refactor into a single-pass dual-stream quarantine pattern "
                    "to preserve failing records in a dedicated quarantine table."
                ),
                line_number=lineno,
                estimated_dbu_impact="Medium (Data Loss Risk)",
                code_snippet=f"@{rule_name}",
            )
        )

    def _emit_dd004(self, node: ast.FunctionDef, fn_name: str, cond_sql: str, lineno: int) -> None:
        """Emit DD004 for non-deterministic functions in expectations."""
        sev = self.config.get_severity("DD004", DiagnosticSeverity.HIGH)
        self.findings.append(
            DiagnosticFinding(
                rule_id="DD004",
                severity=sev,
                title="Non-deterministic function in expectation condition",
                impact_summary=(
                    f"Expectation condition '{cond_sql}' in function '{node.name}' uses "
                    f"non-deterministic function '{fn_name}'. Validation outcomes can shift "
                    "across retries and streaming micro-batches, leading to inconsistent pipelines."
                ),
                recommendation=(
                    f"Replace '{fn_name}' with a deterministic column value or watermarked timestamp."
                ),
                line_number=lineno,
                estimated_dbu_impact="High (Data Quality Risk)",
                code_snippet=cond_sql,
            )
        )

    def _emit_dd003(self, node: ast.FunctionDef, var_name: str, lineno: int) -> None:
        """Emit DD003 for unresolved dynamic expectation dictionaries."""
        sev = self.config.get_severity("DD003", DiagnosticSeverity.INFO)
        self.findings.append(
            DiagnosticFinding(
                rule_id="DD003",
                severity=sev,
                title="Dynamic expectation dictionary cannot be statically verified",
                impact_summary=(
                    f"Expectations passed via variable '{var_name}' in function '{node.name}' "
                    "cannot be statically resolved from source code."
                ),
                recommendation="Review expectations manually; auto-heal is disabled for dynamic expressions.",
                line_number=lineno,
                estimated_dbu_impact="N/A",
                code_snippet=var_name,
            )
        )

    def _audit_quarantine_filters_global(self) -> None:
        """Audit filter calls across the module for hand-written quarantine filters failing NULL safety (DD002)."""
        parsed_exps: list[tuple[str, str, Expr, set[str]]] = []
        all_cols: set[str] = set()

        for r_name, cond in self.all_expectation_conditions:
            try:
                parsed = sqlglot.parse_one(cond, read="spark")
                cols = {c.name.lower() for c in parsed.find_all(sqlglot.exp.Column) if c.name}
                if cols:
                    parsed_exps.append((r_name, cond, parsed, cols))
                    all_cols.update(cols)
            except sqlglot.errors.SqlglotError:
                continue

        if not all_cols:
            return

        quarantine_pattern = re.compile(r"quarantine|invalid|dlq|rejected", re.IGNORECASE)

        for child, table_name in self.filter_calls:
            if not child.args:
                continue
            arg = child.args[0]
            if not (isinstance(arg, ast.Constant) and isinstance(arg.value, str)):
                continue

            filter_sql = arg.value
            try:
                parsed_filter = sqlglot.parse_one(filter_sql, read="spark")
            except sqlglot.errors.SqlglotError:
                continue

            filter_cols = {
                c.name.lower() for c in parsed_filter.find_all(sqlglot.exp.Column) if c.name
            }
            matching_cols = filter_cols.intersection(all_cols)
            if not matching_cols:
                continue

            # Check if this is a valid-side filter (matching the expectation condition itself)
            is_valid_side = False
            for _, _, pe, _ in parsed_exps:
                if parsed_filter == pe:
                    is_valid_side = True
                    break
                try:
                    if simplify(parsed_filter) == simplify(pe):
                        is_valid_side = True
                        break
                except (sqlglot.errors.SqlglotError, ValueError, TypeError):
                    continue

            if is_valid_side:
                continue

            # DD002 only flags filters that feed a table named *quarantine*/*invalid*/*dlq*/*rejected*
            # or that are the syntactic complement of an expectation
            is_quarantine_table = bool(quarantine_pattern.search(table_name))
            is_complement = False
            matched_rule_cond: str | None = None

            for _, cond_sql, pe, cols in parsed_exps:
                if not filter_cols.intersection(cols):
                    continue
                try:
                    s_filter = simplify(parsed_filter)
                    s_not_exp = simplify(sqlglot.parse_one(f"NOT ({cond_sql})", read="spark"))
                    if s_filter == s_not_exp:
                        is_complement = True
                        matched_rule_cond = cond_sql
                        break
                    s_not_filter = simplify(sqlglot.parse_one(f"NOT ({filter_sql})", read="spark"))
                    s_exp = simplify(pe)
                    if s_not_filter == s_exp:
                        is_complement = True
                        matched_rule_cond = cond_sql
                        break
                except (sqlglot.errors.SqlglotError, ValueError, TypeError):
                    continue

            if not (is_quarantine_table or is_complement):
                continue

            # Check for NULL handling
            lowered_filter = filter_sql.lower()
            for col_name in matching_cols:
                has_null_handling = (
                    f"{col_name} is null" in lowered_filter
                    or "coalesce" in lowered_filter
                    or "isnull" in lowered_filter
                    or "is_null" in lowered_filter
                    or "nvl" in lowered_filter
                    or "ifnull" in lowered_filter
                )
                if not has_null_handling:
                    target_cond = matched_rule_cond or filter_sql
                    sev = self.config.get_severity("DD002", DiagnosticSeverity.HIGH)
                    self.findings.append(
                        DiagnosticFinding(
                            rule_id="DD002",
                            severity=sev,
                            title="Quarantine filter is not NULL-safe complement",
                            impact_summary=(
                                f"Filter condition '{filter_sql}' does not handle NULL for column '{col_name}'. "
                                "In SQL three-valued logic, records where "
                                f"'{col_name}' is NULL evaluate to NULL (falsy) and are excluded from "
                                "the quarantine stream as well."
                            ),
                            recommendation=(
                                f"Wrap condition in coalesce(NOT ({target_cond}), true) or explicitly "
                                f"include 'OR {col_name} IS NULL'."
                            ),
                            line_number=child.lineno,
                            estimated_dbu_impact="High (Data Loss Risk)",
                            code_snippet=filter_sql,
                        )
                    )


def audit_sql_source(sql_text: str, config: DoctorConfig | None = None) -> list[DiagnosticFinding]:
    """Scan SQL code for expectation and Unity Catalog anti-patterns without Python AST."""
    findings: list[DiagnosticFinding] = []
    cfg = config or DoctorConfig()

    lines = sql_text.splitlines()
    for idx, line in enumerate(lines, start=1):
        # DD001: ON VIOLATION DROP ROW
        if re.search(r"\bON\s+VIOLATION\s+DROP\s+ROW\b", line, re.IGNORECASE):
            sev = cfg.get_severity("DD001", DiagnosticSeverity.MEDIUM)
            findings.append(
                DiagnosticFinding(
                    rule_id="DD001",
                    severity=sev,
                    title="Silent data loss from ON VIOLATION DROP ROW",
                    impact_summary=(
                        "SQL table expectation specifies 'ON VIOLATION DROP ROW'. Dropped rows are discarded "
                        "without being written to any quarantine table."
                    ),
                    recommendation="Route failing rows to a companion quarantine streaming table.",
                    line_number=idx,
                    code_snippet=line.strip(),
                )
            )

        # UC-001: /mnt/ or dbfs:/
        if "/mnt/" in line or "dbfs:/" in line:
            sev = cfg.get_severity("UC-001", DiagnosticSeverity.HIGH)
            findings.append(
                DiagnosticFinding(
                    rule_id="UC-001",
                    severity=sev,
                    title="Legacy DBFS root mount path detected in SQL",
                    impact_summary="SQL statement references legacy /mnt/ or dbfs:/ path.",
                    recommendation="Migrate storage paths to Unity Catalog Volumes (e.g. /Volumes/catalog/schema/volume).",
                    line_number=idx,
                    code_snippet=line.strip(),
                )
            )

    return findings
