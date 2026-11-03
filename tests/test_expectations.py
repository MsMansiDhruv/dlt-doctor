"""Tests for ExpectationDoctor DLT quarantine routing."""

from dlt_doctor.expectations import ExpectationDoctor, ExpectationRule


def test_build_quarantine_routing() -> None:
    doctor = ExpectationDoctor()
    rules = [
        ExpectationRule("valid_id", "order_id IS NOT NULL"),
        ExpectationRule("positive_amount", "amount > 0"),
    ]
    plan = doctor.build_quarantine_routing("silver_orders", rules)

    assert plan.target_clean_table == "silver_orders"
    assert plan.target_quarantine_table == "silver_orders_quarantine"
    assert "(order_id IS NOT NULL) AND (amount > 0)" in plan.valid_filter_sql
    assert "NOT ((order_id IS NOT NULL) AND (amount > 0))" in plan.invalid_filter_sql


def test_generate_dlt_decorator_code() -> None:
    doctor = ExpectationDoctor()
    rules = [ExpectationRule("valid_email", "email LIKE '%@%.%'")]
    plan = doctor.build_quarantine_routing("dim_customers", rules)
    code = doctor.generate_dlt_decorator_code(plan)

    assert "@dlt.table(" in code
    assert "def dim_customers():" in code
    assert "def dim_customers_quarantine():" in code
