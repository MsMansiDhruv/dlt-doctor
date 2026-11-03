"""Tests for SkewDoctor join key analysis."""

from dlt_doctor.skew import SkewDoctor


def test_skew_doctor_detects_severe_skew() -> None:
    doctor = SkewDoctor()
    # Key 'guest_checkout' has 500,000 occurrences while median is 1,000 (500x ratio)
    distribution = {
        "user_1": 1000,
        "user_2": 950,
        "user_3": 1100,
        "guest_checkout": 500000,
    }
    result = doctor.analyze_key_distribution(distribution, skew_ratio_threshold=5.0)

    assert result.has_severe_skew is True
    assert "guest_checkout" in result.skewed_keys
    assert result.max_to_median_ratio > 400.0
    assert result.recommended_configs["spark.sql.adaptive.skewJoin.enabled"] == "true"
    assert "F.floor(F.rand() * 4)" in result.salting_code_snippet


def test_skew_doctor_balanced_keys() -> None:
    doctor = SkewDoctor()
    distribution = {
        "k1": 100,
        "k2": 110,
        "k3": 95,
        "k4": 105,
    }
    result = doctor.analyze_key_distribution(distribution, skew_ratio_threshold=5.0)

    assert result.has_severe_skew is False
    assert result.skewed_keys == []
    assert len(result.findings) == 0
