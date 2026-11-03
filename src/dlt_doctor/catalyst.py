"""PySpark DataFrame and Catalyst execution plan diagnostics."""

from __future__ import annotations

import ast
import enum
from dataclasses import dataclass
from typing import Any


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
    line_number: int | None = None
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

    def __init__(self) -> None:
        self.findings: list[DiagnosticFinding] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        # Check for @udf decorator
        for decorator in node.decorator_list:
            dec_name = ""
            if isinstance(decorator, ast.Name):
                dec_name = decorator.id
            elif isinstance(decorator, ast.Call) and isinstance(
                decorator.func, ast.Name
            ):
                dec_name = decorator.func.id
            elif isinstance(decorator, ast.Attribute):
                dec_name = decorator.attr
            elif isinstance(decorator, ast.Call) and isinstance(
                decorator.func, ast.Attribute
            ):
                dec_name = decorator.func.attr

            if dec_name.lower() == "udf":
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
                        line_number=node.lineno,
                        estimated_dbu_impact="High (10x-50x slower processing)",
                        code_snippet=f"def {node.name}(...)",
                    )
                )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
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
            # If reducing partitions or used without column, advise coalesce
            if len(node.args) == 1 and isinstance(node.args[0], ast.Constant):
                self.findings.append(
                    DiagnosticFinding(
                        rule_id="PYSPARK-004",
                        severity=DiagnosticSeverity.MEDIUM,
                        title="Repartition triggers full shuffle",
                        impact_summary=(
                            f".repartition({node.args[0].value}) triggers a full round-robin shuffle "
                            "across cluster network."
                        ),
                        recommendation=(
                            "If reducing partition count before writing to Delta Lake, "
                            f"use .coalesce({node.args[0].value}) to avoid a network shuffle."
                        ),
                        line_number=node.lineno,
                        estimated_dbu_impact="Medium",
                    )
                )

        # Check for chained withColumn calls
        if isinstance(node.func, ast.Attribute) and node.func.attr == "withColumn":
            chain_depth = self._count_with_column_depth(node)
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

    def _count_with_column_depth(self, node: ast.Call) -> int:
        count = 1
        curr = node
        while isinstance(curr.func, ast.Attribute) and curr.func.attr == "withColumn":
            if isinstance(curr.func.value, ast.Call):
                curr = curr.func.value
                count += 1
            else:
                break
        return count


class PlanDoctor:
    """Diagnoses Catalyst execution plans and PySpark code patterns."""

    def __init__(self) -> None:
        pass

    def analyze_source_code(self, source_code: str) -> list[DiagnosticFinding]:
        """Statically inspect PySpark source code without starting a JVM cluster."""
        try:
            tree = ast.parse(source_code)
        except SyntaxError as e:
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

        visitor = PySparkCodeVisitor()
        visitor.visit(tree)

        # Check for missing cache on repeated actions
        action_findings = self._detect_repeated_actions(source_code)
        visitor.findings.extend(action_findings)

        return visitor.findings

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

        for idx, line in enumerate(lines, 1):
            if any(act in line for act in action_names):
                action_calls += 1

        if (
            action_calls >= 3
            and ".cache()" not in source_code
            and ".persist(" not in source_code
        ):
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
