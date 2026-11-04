"""Tests for GitHub Action specification (action.yml)."""

from __future__ import annotations

from pathlib import Path


def test_action_yml_structure() -> None:
    action_path = Path(__file__).parent.parent / "action.yml"
    assert action_path.exists(), "action.yml must exist at repository root"

    content = action_path.read_text(encoding="utf-8")

    # Verify required inputs are defined
    required_inputs = [
        "path:",
        "format:",
        "fail-on:",
        "config:",
        "sarif-output:",
        "python-version:",
        "working-directory:",
    ]
    for inp in required_inputs:
        assert inp in content, f"Missing input definition '{inp}' in action.yml"

    # Verify inputs are passed via environment variables to prevent script injection
    assert "INPUT_PATH:" in content
    assert "INPUT_FAIL_ON:" in content
    assert "INPUT_FORMAT:" in content
    assert "INPUT_CONFIG:" in content
    assert "INPUT_SARIF_OUTPUT:" in content

    # Verify step summary integration
    assert "GITHUB_STEP_SUMMARY" in content

    # Verify SARIF output handling
    assert "--format sarif" in content
