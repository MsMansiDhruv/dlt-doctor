"""Official rule definitions registered in dlt-doctor."""

from __future__ import annotations

from dlt_doctor.catalyst import DiagnosticSeverity
from dlt_doctor.rules.registry import Rule, register

# Existing PySpark AST rules
register(
    Rule(
        id="PYSPARK-001",
        name="Python UDF serialization overhead",
        default_severity=DiagnosticSeverity.CRITICAL,
        description="Row-by-row JVM-to-Python socket serialization disables Catalyst vectorization.",
        docs_slug="catalyst#pyspark-001",
    )
)
register(
    Rule(
        id="PYSPARK-002",
        name="Deep withColumn cascade",
        default_severity=DiagnosticSeverity.HIGH,
        description="Chained .withColumn() calls create deep Project AST chains leading to compiler degradation.",
        docs_slug="catalyst#pyspark-002",
    )
)
register(
    Rule(
        id="PYSPARK-004",
        name="Repartition triggers full shuffle",
        default_severity=DiagnosticSeverity.MEDIUM,
        description="Explicit .repartition() triggers a full round-robin shuffle across workers.",
        docs_slug="catalyst#pyspark-004",
    )
)
register(
    Rule(
        id="PYSPARK-005",
        name="Multiple action calls without DataFrame caching",
        default_severity=DiagnosticSeverity.HIGH,
        description="Multiple actions on unpersisted DataFrame recompute upstream lineage from source.",
        docs_slug="catalyst#pyspark-005",
    )
)
register(
    Rule(
        id="PYSPARK-006",
        name="Explicit Cartesian crossJoin",
        default_severity=DiagnosticSeverity.CRITICAL,
        description="crossJoin produces M * N record multiplication, risking executor OOM and spill.",
        docs_slug="catalyst#pyspark-006",
    )
)

# Syntax rules
register(
    Rule(
        id="SYNTAX-001",
        name="Source syntax error",
        default_severity=DiagnosticSeverity.CRITICAL,
        description="Source file fails language parser validation.",
        docs_slug="syntax#syntax-001",
    )
)

# Physical Plan rules
register(
    Rule(
        id="PLAN-001",
        name="Catalyst CartesianProduct operator in execution plan",
        default_severity=DiagnosticSeverity.CRITICAL,
        description="Physical plan contains CartesianProduct operator without join condition.",
        docs_slug="catalyst#plan-001",
    )
)
register(
    Rule(
        id="PLAN-002",
        name="SortMergeJoin utilized without Broadcast hints",
        default_severity=DiagnosticSeverity.MEDIUM,
        description="SortMergeJoin executed where BroadcastExchange was feasible on smaller tables.",
        docs_slug="catalyst#plan-002",
    )
)
register(
    Rule(
        id="PLAN-003",
        name="BatchEvalPython operator detected in physical plan",
        default_severity=DiagnosticSeverity.HIGH,
        description="Catalyst is shipping rows across socket to Python worker daemon.",
        docs_slug="catalyst#plan-003",
    )
)
register(
    Rule(
        id="PLAN-004",
        name="Empty PartitionFilters on partitioned table scan",
        default_severity=DiagnosticSeverity.HIGH,
        description="Query scans partitioned table without partition pruning predicates.",
        docs_slug="catalyst#plan-004",
    )
)

# Unity Catalog migration rules
register(
    Rule(
        id="UC-001",
        name="Legacy DBFS root mount path detected",
        default_severity=DiagnosticSeverity.HIGH,
        description="Code references deprecated /mnt/ or dbfs:/ paths not secured by Unity Catalog.",
        docs_slug="uc#uc-001",
    )
)
register(
    Rule(
        id="UC-002",
        name="Legacy two-tier Hive Metastore table reference",
        default_severity=DiagnosticSeverity.MEDIUM,
        description="Table references lack 3-level catalog namespace (catalog.schema.table).",
        docs_slug="uc#uc-002",
    )
)
register(
    Rule(
        id="UC-003",
        name="Hardcoded workspace local file path",
        default_severity=DiagnosticSeverity.HIGH,
        description="Direct local filesystem references (/tmp, /local) will fail across distributed workers.",
        docs_slug="uc#uc-003",
    )
)
register(
    Rule(
        id="UC-004",
        name="Legacy DBFS mount API call detected",
        default_severity=DiagnosticSeverity.HIGH,
        description="dbutils.fs.mount() is deprecated in Unity Catalog workspaces.",
        docs_slug="uc#uc-004",
    )
)

# New DD Rules (Data Quality & Lakeflow)
register(
    Rule(
        id="DD001",
        name="Silent data loss from expect_or_drop policy",
        default_severity=DiagnosticSeverity.MEDIUM,
        description=(
            "expect_or_drop silently drops non-compliant records from target tables. "
            "Dropped rows' contents are not persisted anywhere in the pipeline (only aggregate "
            "counts are logged). Use dual-stream quarantine routing to preserve invalid records."
        ),
        docs_slug="expectations#dd001",
    )
)
register(
    Rule(
        id="DD002",
        name="Quarantine filter is not NULL-safe complement",
        default_severity=DiagnosticSeverity.HIGH,
        description=(
            "Quarantine filter does not handle SQL three-valued logic (NULL) for referenced columns. "
            "When a condition evaluates to NULL, both target and quarantine filters discard the row."
        ),
        docs_slug="expectations#dd002",
    )
)
register(
    Rule(
        id="DD003",
        name="Dynamic expectation dictionary cannot be statically verified",
        default_severity=DiagnosticSeverity.INFO,
        description=(
            "Expectations are defined dynamically via runtime variables or external sources. "
            "Static analysis cannot resolve the expectation conditions."
        ),
        docs_slug="expectations#dd003",
    )
)
