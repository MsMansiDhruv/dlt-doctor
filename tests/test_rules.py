"""Tests for rule registry and rules DD001, DD002, DD003."""


from dlt_doctor.catalyst import DiagnosticSeverity, PlanDoctor
from dlt_doctor.expectations_rules import audit_sql_source
from dlt_doctor.rules import get_rule, list_rules, rule_exists


def test_rule_registry_contains_all_rules() -> None:
    rules = list_rules()
    assert len(rules) >= 15
    rule_ids = {r.id for r in rules}
    expected_ids = {
        "PYSPARK-001", "PYSPARK-002", "PYSPARK-004", "PYSPARK-005", "PYSPARK-006",
        "SYNTAX-001", "PLAN-001", "PLAN-002", "PLAN-003", "PLAN-004",
        "UC-001", "UC-002", "UC-003", "UC-004",
        "DD001", "DD002", "DD003",
    }
    assert expected_ids.issubset(rule_ids)

    # Case-insensitive lookup
    r1 = get_rule("dd001")
    assert r1 is not None
    assert r1.id == "DD001"
    assert r1.default_severity == DiagnosticSeverity.MEDIUM
    assert rule_exists("PYSPARK-001")
    assert not rule_exists("NONEXISTENT-999")


def test_dd001_dlt_and_dp_flavors() -> None:
    code_dlt = """
import dlt

@dlt.table
@dlt.expect_or_drop("valid_val", "val > 0")
def clean_records():
    return dlt.read_stream("raw")
"""
    doctor = PlanDoctor()
    findings_dlt = doctor.analyze_source_code(code_dlt)
    dd001_dlt = [f for f in findings_dlt if f.rule_id == "DD001"]
    assert len(dd001_dlt) == 1
    assert dd001_dlt[0].severity == DiagnosticSeverity.MEDIUM
    assert "Dropped rows' contents are not persisted anywhere" in dd001_dlt[0].impact_summary

    code_dp = """
from pyspark import pipelines as dp

@dp.table(name="clean_orders")
@dp.expect_or_drop("valid_amt", "amt > 0")
def clean_orders():
    return dp.read_stream("raw")
"""
    # clean_orders matches regulated table pattern "*orders*"
    findings_dp = doctor.analyze_source_code(code_dp)
    dd001_dp = [f for f in findings_dp if f.rule_id == "DD001"]
    assert len(dd001_dp) == 1
    assert dd001_dp[0].severity == DiagnosticSeverity.HIGH  # escalated because regulated table


def test_dd001_regulated_comment_escalation() -> None:
    code = """
import dlt

# dlt-doctor: regulated
@dlt.table(name="custom_feed")
@dlt.expect_or_drop("valid_feed", "score > 0")
def custom_feed():
    return dlt.read_stream("raw")
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code)
    dd001 = [f for f in findings if f.rule_id == "DD001"]
    assert len(dd001) == 1
    assert dd001[0].severity == DiagnosticSeverity.HIGH


def test_dd001_sql_on_violation_drop_row() -> None:
    sql = """
    CREATE OR REFRESH STREAMING TABLE silver_orders (
        CONSTRAINT valid_amt EXPECT (amt > 0) ON VIOLATION DROP ROW
    ) AS SELECT * FROM STREAM(bronze_orders);
    """
    findings = audit_sql_source(sql)
    dd001 = [f for f in findings if f.rule_id == "DD001"]
    assert len(dd001) == 1
    assert dd001[0].rule_id == "DD001"
    assert "ON VIOLATION DROP ROW" in dd001[0].title


def test_dd002_quarantine_filter_null_safe_check() -> None:
    # True positive: filter does NOT handle NULL
    code_bad = """
import dlt

@dlt.table
@dlt.expect_or_drop("positive_amount", "amount > 0")
def orders():
    return dlt.read_stream("raw")

@dlt.table
def orders_quarantine():
    return dlt.read_stream("raw").filter("amount <= 0")
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code_bad)
    dd002_bad = [f for f in findings if f.rule_id == "DD002"]
    assert len(dd002_bad) == 1
    assert "does not handle NULL for column 'amount'" in dd002_bad[0].impact_summary

    # False positive prevention: filter DOES handle NULL
    code_good = """
import dlt

@dlt.table
@dlt.expect_or_drop("positive_amount", "amount > 0")
def orders():
    return dlt.read_stream("raw")

@dlt.table
def orders_quarantine():
    return dlt.read_stream("raw").filter("amount <= 0 OR amount IS NULL")
"""
    findings_good = doctor.analyze_source_code(code_good)
    dd002_good = [f for f in findings_good if f.rule_id == "DD002"]
    assert len(dd002_good) == 0


def test_dd002_never_flags_valid_side_filters() -> None:
    """DD002 must never flag valid-side filters."""
    code = """
import dlt

@dlt.table
@dlt.expect_or_drop("positive_amount", "amount > 0")
def orders():
    return dlt.read_stream("raw").filter("amount > 0")
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code)
    dd002 = [f for f in findings if f.rule_id == "DD002"]
    assert len(dd002) == 0, f"Expected 0 DD002 on valid-side filter, got {len(dd002)}"


def test_dd002_uses_token_matching_not_substrings() -> None:
    """DD002 must not flag filters matching column names as substrings."""
    code = """
import dlt

@dlt.table
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
def orders():
    return dlt.read_stream("raw")

@dlt.table
def orders_quarantine():
    return dlt.read_stream("raw").filter("valid_identifier = true")
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code)
    dd002 = [f for f in findings if f.rule_id == "DD002"]
    assert len(dd002) == 0, f"Expected 0 DD002 for substring column name match, got {len(dd002)}"


def test_dd002_recommendation_and_table_name_matching() -> None:
    """DD002 flags filters feeding *quarantine*/*invalid*/*dlq*/*rejected* and recommends coalesce/IS NULL."""
    code = """
import dlt

@dlt.table
@dlt.expect_or_drop("valid_amt", "amount > 0")
def orders():
    return dlt.read_stream("raw")

@dlt.table(name="silver_dlq")
def dlq():
    return dlt.read_stream("raw").filter("amount <= 0")
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code)
    dd002 = [f for f in findings if f.rule_id == "DD002"]
    assert len(dd002) == 1
    rec = dd002[0].recommendation
    assert ("coalesce(NOT (" in rec or "OR amount IS NULL" in rec)


def test_dd002_flags_syntactic_complement_in_unnamed_table() -> None:
    """DD002 flags filter that is the syntactic complement of expectation even in non-quarantine named table."""
    code = """
import dlt

@dlt.table
@dlt.expect_or_drop("valid_amt", "amount > 0")
def orders():
    return dlt.read_stream("raw")

@dlt.table
def sink():
    return dlt.read_stream("raw").filter("NOT (amount > 0)")
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code)
    dd002 = [f for f in findings if f.rule_id == "DD002"]
    assert len(dd002) == 1
    assert "amount" in dd002[0].impact_summary


def test_dd002_ignores_quarantine_with_coalesce_null_handling() -> None:
    """DD002 does not flag quarantine filters using coalesce."""
    code = """
import dlt

@dlt.table
@dlt.expect_or_drop("valid_amt", "amount > 0")
def orders():
    return dlt.read_stream("raw")

@dlt.table
def orders_quarantine():
    return dlt.read_stream("raw").filter("coalesce(NOT (amount > 0), true)")
"""
    doctor = PlanDoctor()
    findings = doctor.analyze_source_code(code)
    dd002 = [f for f in findings if f.rule_id == "DD002"]
    assert len(dd002) == 0



def test_dd003_static_vs_dynamic_expectations() -> None:
    # Statically resolvable module dictionary -> no DD003
    code_static = """
import dlt

MY_RULES = {"rule1": "x > 0", "rule2": "y IS NOT NULL"}

@dlt.table
@dlt.expect_all_or_drop(MY_RULES)
def clean_tbl():
    return dlt.read_stream("raw")
"""
    doctor = PlanDoctor()
    findings_static = doctor.analyze_source_code(code_static)
    dd003_static = [f for f in findings_static if f.rule_id == "DD003"]
    assert len(dd003_static) == 0

    # Dynamic unresolved expectations -> emits DD003 (INFO)
    code_dynamic = """
import dlt

@dlt.table
@dlt.expect_all_or_drop(load_dynamic_rules_from_catalog())
def clean_tbl():
    return dlt.read_stream("raw")
"""
    findings_dynamic = doctor.analyze_source_code(code_dynamic)
    dd003_dynamic = [f for f in findings_dynamic if f.rule_id == "DD003"]
    assert len(dd003_dynamic) == 1
    assert dd003_dynamic[0].severity == DiagnosticSeverity.INFO
    assert "cannot be statically resolved" in dd003_dynamic[0].impact_summary
