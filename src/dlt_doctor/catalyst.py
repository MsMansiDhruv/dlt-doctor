"""PySpark DataFrame and Catalyst execution plan diagnostics."""

from __future__ import annotations

import ast
import enum
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dlt_doctor.config import DoctorConfig


class DiagnosticSeverity(str, enum.Enum):
    """Severity classification for pipeline bottlenecks."""

    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


@dataclass
class DiagnosticFinding:
    """An individual performance or anti-pattern bottleneck identified."""

    rule_id: str
    severity: DiagnosticSeverity
    title: str
    impact_summary: str
    recommendation: str
    line_number: int | str | None = None
    estimated_dbu_impact: str = "Low"
    code_snippet: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "severity": self.severity.value,
            "title": self.title,
            "impact_summary": self.impact_summary,
            "recommendation": self.recommendation,
            "line_number": self.line_number,
            "estimated_dbu_impact": self.estimated_dbu_impact,
            "code_snippet": self.code_snippet,
        }


class PySparkCodeVisitor(ast.NodeVisitor):
    """AST visitor traversing PySpark source code for anti-patterns."""

    def __init__(self, config: DoctorConfig | None = None) -> None:
        self.findings: list[DiagnosticFinding] = []
        self.config = config
        self._decorator_call_ids: set[int] = set()
        self._processed_with_column_ids: set[int] = set()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        # Register decorator calls to avoid duplicate visit_Call findings
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                self._decorator_call_ids.add(id(decorator))
            dec_name = ""
            if isinstance(decorator, ast.Name):
                dec_name = decorator.id
            elif isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name):
                dec_name = decorator.func.id
            elif isinstance(decorator, ast.Attribute):
                dec_name = decorator.attr
            elif isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute):
                dec_name = decorator.func.attr

            if dec_name.lower() == "udf":
                dec_lineno = getattr(decorator, "lineno", node.lineno)
                self.findings.append(
                    DiagnosticFinding(
                        rule_id="PYSPARK-001",
                        severity=DiagnosticSeverity.CRITICAL,
                        title="Python UDF serialization overhead",
                        impact_summary=(
                            f"Function '{node.name}' uses standard Python @udf. "
                            "Forces row-by-row JVM-to-Python socket serialization, "
                            "disabling Whole-Stage CodeGen and Catalyst vectorization."
                        ),
                        recommendation=(
                            "Rewrite using native PySpark built-in functions (F.when, F.coalesce), "
                            "Spark SQL expressions, or an Apache Arrow vectorized @pandas_udf."
                        ),
                        line_number=dec_lineno,
                        estimated_dbu_impact="High (10x-50x slower processing)",
                        code_snippet=f"def {node.name}(...)",
                    )
                )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        # Skip if this call is part of a function decorator (already handled in visit_FunctionDef)
        if id(node) in self._decorator_call_ids:
            self.generic_visit(node)
            return

        # Check for udf(...) function call assignment
        if isinstance(node.func, ast.Name) and node.func.id.lower() == "udf":
            self.findings.append(
                DiagnosticFinding(
                    rule_id="PYSPARK-001",
                    severity=DiagnosticSeverity.CRITICAL,
                    title="Inline Python UDF assignment",
                    impact_summary=(
                        "udf(...) call forces Python daemon IPC serialization across workers."
                    ),
                    recommendation="Replace with native F functions or @pandas_udf.",
                    line_number=node.lineno,
                    estimated_dbu_impact="High",
                )
            )

        # Check for .crossJoin()
        if isinstance(node.func, ast.Attribute) and node.func.attr == "crossJoin":
            self.findings.append(
                DiagnosticFinding(
                    rule_id="PYSPARK-006",
                    severity=DiagnosticSeverity.CRITICAL,
                    title="Explicit Cartesian crossJoin",
                    impact_summary=(
                        "crossJoin produces M * N record multiplication, risking executor OOM "
                        "and massive shuffle spill to disk."
                    ),
                    recommendation="Ensure a selective equi-join condition is provided.",
                    line_number=node.lineno,
                    estimated_dbu_impact="Critical",
                )
            )

        # Check for .repartition() right before write
        if isinstance(node.func, ast.Attribute) and node.func.attr == "repartition":
            if (
                len(node.args) == 1
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == 1
            ):
                self.findings.append(
                    DiagnosticFinding(
                        rule_id="PYSPARK-004",
                        severity=DiagnosticSeverity.MEDIUM,
                        title="Repartition triggers full shuffle",
                        impact_summary=(
                            ".repartition(1) triggers a full round-robin shuffle "
                            "across cluster network."
                        ),
                        recommendation=(
                            "If reducing partition count to 1 before writing to Delta Lake, "
                            "use .coalesce(1) to avoid a network shuffle."
                        ),
                        line_number=node.lineno,
                        estimated_dbu_impact="Medium",
                    )
                )
            elif len(node.args) >= 1:
                coalesce_hint = ".coalesce()"
                if isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, int):
                    coalesce_hint = f".coalesce({node.args[0].value})"
                self.findings.append(
                    DiagnosticFinding(
                        rule_id="PYSPARK-004",
                        severity=DiagnosticSeverity.INFO,
                        title="Explicit partition count shuffle",
                        impact_summary=(
                            "Explicit .repartition() call forces a shuffle across cluster network."
                        ),
                        recommendation=(
                            f"If reducing partitions before write, consider {coalesce_hint} to avoid network shuffle; "
                            "if increasing parallelism or redistributing data, .repartition() is expected."
                        ),
                        line_number=node.lineno,
                        estimated_dbu_impact="Low",
                    )
                )

        # Check for chained withColumn calls (only report once on outermost call in chain)
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "withColumn"
            and id(node) not in self._processed_with_column_ids
        ):
            chain = [node]
            curr = node
            while isinstance(curr.func, ast.Attribute) and curr.func.attr == "withColumn":
                if (
                    isinstance(curr.func.value, ast.Call)
                    and isinstance(curr.func.value.func, ast.Attribute)
                    and curr.func.value.func.attr == "withColumn"
                ):
                    curr = curr.func.value
                    chain.append(curr)
                else:
                    break
            for c in chain:
                self._processed_with_column_ids.add(id(c))

            chain_depth = len(chain)
            if chain_depth >= 4:
                self.findings.append(
                    DiagnosticFinding(
                        rule_id="PYSPARK-002",
                        severity=DiagnosticSeverity.HIGH,
                        title="Deep withColumn cascade",
                        impact_summary=(
                            f"Detected {chain_depth} chained .withColumn() calls. "
                            "Each call adds a Project node to Catalyst query plan, "
                            "causing linear plan compilation degradation and potential StackOverflowError."
                        ),
                        recommendation=(
                            "Consolidate into a single df.withColumns({...}) call (Spark 3.3+) "
                            "or a single df.select('*', ...)."
                        ),
                        line_number=node.lineno,
                        estimated_dbu_impact="High",
                    )
                )

        self.generic_visit(node)


class PlanDoctor:
    """Diagnoses Catalyst execution plans and PySpark code patterns."""

    def __init__(self) -> None:
        pass

    def analyze_source_code(
        self,
        source_code: str,
        config: DoctorConfig | None = None,
        file_name: str | None = None,
    ) -> list[DiagnosticFinding]:
        """Statically inspect PySpark / Lakeflow source code without starting a JVM cluster."""
        from dlt_doctor.expectations_rules import ExpectationAstVisitor

        file_ignores: set[str] = set()
        for line in source_code.splitlines():
            m = re.search(r"#\s*dlt-doctor:\s*ignore-file\[([^\]]+)\]", line)
            if m:
                for r in m.group(1).split(","):
                    file_ignores.add(r.strip().upper())

        # If file is SQL or starts with SQL keyword, audit via SQL scanner
        if (file_name and file_name.lower().endswith(".sql")) or re.match(
            r"^\s*(SELECT|CREATE|MERGE|WITH|INSERT|ALTER|DROP)\b",
            source_code,
            re.IGNORECASE,
        ):
            from dlt_doctor.expectations_rules import audit_sql_source

            return audit_sql_source(source_code, config=config)

        try:
            tree = ast.parse(source_code)
        except SyntaxError as e:
            if "SYNTAX-001" in file_ignores:
                return []
            return [
                DiagnosticFinding(
                    rule_id="SYNTAX-001",
                    severity=DiagnosticSeverity.CRITICAL,
                    title="Python Syntax Error",
                    impact_summary=str(e),
                    recommendation="Correct Python syntax before running pipeline.",
                    line_number=e.lineno,
                    estimated_dbu_impact="N/A",
                )
            ]

        visitor = PySparkCodeVisitor(config=config)
        visitor.visit(tree)

        # Audit expectation patterns (DD001, DD002, DD003)
        exp_visitor = ExpectationAstVisitor(source_code, config=config)
        exp_visitor.visit(tree)
        exp_visitor.finalize()
        visitor.findings.extend(exp_visitor.findings)

        # Check for missing cache on repeated actions
        action_findings = self._detect_repeated_actions(source_code)
        visitor.findings.extend(action_findings)

        # Apply inline suppressions (# dlt-doctor: ignore[RULE_ID])
        lines = source_code.splitlines()
        filtered_findings: list[DiagnosticFinding] = []
        for finding in visitor.findings:
            if finding.rule_id.upper() in file_ignores:
                continue
            if config and not config.is_rule_enabled(finding.rule_id, file_name):
                continue
            is_suppressed = False
            if finding.line_number:
                cand_lines: list[int] = []
                if isinstance(finding.line_number, int):
                    cand_lines = [finding.line_number]
                else:
                    m_num = re.search(r"line\s+(\d+)", str(finding.line_number))
                    if m_num:
                        cand_lines = [int(m_num.group(1))]

                for l_num in cand_lines:
                    for offset in (0, -1, 1):
                        idx = l_num - 1 + offset
                        if 0 <= idx < len(lines):
                            m = re.search(r"#\s*dlt-doctor:\s*ignore\[([^\]]+)\]", lines[idx])
                            if m:
                                suppressed_rules = [
                                    r.strip().upper() for r in m.group(1).split(",")
                                ]
                                if (
                                    finding.rule_id.upper() in suppressed_rules
                                    or "ALL" in suppressed_rules
                                ):
                                    is_suppressed = True
                                    break
                    if is_suppressed:
                        break

            if is_suppressed:
                continue
            filtered_findings.append(finding)

        return filtered_findings

    def analyze_plan_string(self, plan_str: str) -> list[DiagnosticFinding]:
        """Analyze Catalyst explain plan output (from df.explain(True) or .toString())."""
        findings: list[DiagnosticFinding] = []
        lowered = plan_str.lower()

        # Check for CartesianProduct
        if "cartesianproduct" in lowered:
            findings.append(
                DiagnosticFinding(
                    rule_id="PLAN-001",
                    severity=DiagnosticSeverity.CRITICAL,
                    title="Catalyst CartesianProduct operator in execution plan",
                    impact_summary=(
                        "Physical plan contains CartesianProduct without join condition. "
                        "Causes quadratic memory explosion and massive shuffle spill."
                    ),
                    recommendation="Add explicit join keys or filter predicates.",
                    estimated_dbu_impact="Critical",
                )
            )

        # Check for SortMergeJoin on small tables where BroadcastExchange was possible
        if "sortmergejoin" in lowered and "broadcastnestedloopjoin" not in lowered:
            findings.append(
                DiagnosticFinding(
                    rule_id="PLAN-002",
                    severity=DiagnosticSeverity.MEDIUM,
                    title="SortMergeJoin utilized without Broadcast hints",
                    impact_summary=(
                        "Plan uses SortMergeJoin requiring full shuffle and sort on both sides. "
                        "If one table is a small dimension table (< 100MB), this burns cluster network bandwidth."
                    ),
                    recommendation="Wrap dimension DataFrame in broadcast(dim_df).",
                    estimated_dbu_impact="Medium",
                )
            )

        # Check for BatchEvalPython (indicates standard Python UDF executed)
        if "batchevalpython" in lowered:
            findings.append(
                DiagnosticFinding(
                    rule_id="PLAN-003",
                    severity=DiagnosticSeverity.HIGH,
                    title="BatchEvalPython operator detected in physical plan",
                    impact_summary=(
                        "Catalyst is shipping rows across socket to Python worker daemon. "
                        "Disables native Tungsten off-heap memory processing."
                    ),
                    recommendation="Replace Python UDF with native Spark expressions or Arrow Pandas UDF.",
                    estimated_dbu_impact="High",
                )
            )

        # Check for Full Table Scan on partitioned table without PartitionFilters
        if "partitionfilters: []" in lowered or "partitionfilters: [none]" in lowered:
            findings.append(
                DiagnosticFinding(
                    rule_id="PLAN-004",
                    severity=DiagnosticSeverity.HIGH,
                    title="Empty PartitionFilters on partitioned table scan",
                    impact_summary=(
                        "Query scans partitioned table without partition pruning. "
                        "Catalyst will scan all partition directories and Parquet files."
                    ),
                    recommendation="Add filter predicates on the table partition column.",
                    estimated_dbu_impact="High",
                )
            )

        return findings

    def _detect_repeated_actions(self, source_code: str) -> list[DiagnosticFinding]:
        findings: list[DiagnosticFinding] = []
        lines = source_code.splitlines()
        action_names = [".count()", ".show(", ".collect()", ".take("]
        action_calls = 0

        for _idx, line in enumerate(lines, 1):
            if any(act in line for act in action_names):
                action_calls += 1

        if action_calls >= 3 and ".cache()" not in source_code and ".persist(" not in source_code:
            findings.append(
                DiagnosticFinding(
                    rule_id="PYSPARK-005",
                    severity=DiagnosticSeverity.HIGH,
                    title="Multiple action calls without DataFrame caching",
                    impact_summary=(
                        f"Detected {action_calls} action calls (.count/.show/.collect) "
                        "without .cache() or .persist(). Upstream pipeline lineage will re-execute from source."
                    ),
                    recommendation=(
                        "Persist the intermediate DataFrame before running multiple actions: "
                        "df.persist(StorageLevel.MEMORY_AND_DISK)."
                    ),
                    estimated_dbu_impact="High",
                )
            )

        return findings
