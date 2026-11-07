"""GitHub Action entrypoint and runner for dlt-doctor.

Runs analysis once in JSON mode and derives console output, GitHub Step Summary
Markdown, and optional SARIF 2.1.0 output without duplicate pipeline scans.
"""

from __future__ import annotations

import json
import os
import subprocess  # nosec B404
import sys
from pathlib import Path
from typing import Any

SEVERITY_RANK = {
    "CRITICAL": 5,
    "HIGH": 4,
    "MEDIUM": 3,
    "LOW": 2,
    "INFO": 1,
}


def derive_summary_markdown(data: dict[str, Any]) -> str:
    """Generate Markdown report for GitHub Actions Step Summary or PR comments."""
    lines: list[str] = []
    if "file_reports" in data:
        # Directory report
        root = data.get("root_path", ".")
        score = data.get("overall_health_score", 100)
        total = data.get("total_files_scanned", 0)
        clean = data.get("clean_files_count", 0)
        flagged = data.get("flagged_files_count", 0)

        lines.extend(
            [
                f"## DLT-Doctor Multi-Pipeline Audit: `{root}`",
                "",
                f"- **Overall Health Score**: **{score}/100**",
                f"- **Files Scanned**: {total} ({clean} clean, {flagged} flagged)",
                "",
                "| File | Health Score | Critical | High | Medium | Low |",
                "| :--- | :---: | :---: | :---: | :---: | :---: |",
            ]
        )

        for rep in data.get("file_reports", []):
            name = rep.get("pipeline_name", "unknown")
            rep_score = rep.get("overall_health_score", 100)
            crit = sum(1 for f in rep.get("findings", []) if f.get("severity") == "CRITICAL")
            high = sum(1 for f in rep.get("findings", []) if f.get("severity") == "HIGH")
            med = sum(1 for f in rep.get("findings", []) if f.get("severity") == "MEDIUM")
            low = sum(1 for f in rep.get("findings", []) if f.get("severity") == "LOW")
            lines.append(f"| `{name}` | {rep_score} | {crit} | {high} | {med} | {low} |")

        # Detail flagged files
        if flagged > 0:
            lines.extend(["", "### Optimization Findings"])
            for rep in data.get("file_reports", []):
                for f in rep.get("findings", []):
                    lines.append(
                        f"- **[{f.get('severity')}] {f.get('title')} ({f.get('rule_id')})** in `{rep.get('pipeline_name')}`"
                    )
                    if f.get("line_number"):
                        lines.append(f"  - Line: `{f.get('line_number')}`")
                    lines.append(f"  - Impact: {f.get('impact_summary')}")
                    lines.append(f"  - Fix: {f.get('recommendation')}")
    else:
        # Single file report
        name = data.get("pipeline_name", "pipeline")
        score = data.get("overall_health_score", 100)
        findings = data.get("findings", [])

        lines.extend(
            [
                f"## DLT-Doctor Audit: `{name}`",
                "",
                f"- **Overall Health Score**: **{score}/100**",
                f"- **Findings Detected**: {len(findings)}",
                "",
            ]
        )

        if findings:
            lines.extend(
                [
                    "| Severity | Rule | Title | Line | Recommendation |",
                    "| :--- | :--- | :--- | :--- | :--- |",
                ]
            )
            for f in findings:
                lines.append(
                    f"| {f.get('severity')} | {f.get('rule_id')} | {f.get('title')} | "
                    f"{f.get('line_number') or '-'} | {f.get('recommendation')} |"
                )

    return "\n".join(lines)


def derive_sarif(data: dict[str, Any]) -> dict[str, Any]:
    """Convert JSON analysis data into SARIF 2.1.0 payload."""
    results: list[dict[str, Any]] = []
    rules_map: dict[str, dict[str, Any]] = {}

    findings_with_file: list[tuple[str, dict[str, Any]]] = []
    if "file_reports" in data:
        for rep in data.get("file_reports", []):
            fname = rep.get("pipeline_name", "unknown")
            for f in rep.get("findings", []):
                findings_with_file.append((fname, f))
    else:
        fname = data.get("pipeline_name", "pipeline")
        for f in data.get("findings", []):
            findings_with_file.append((fname, f))

    for fname, f in findings_with_file:
        rule_id = f.get("rule_id", "DD999")
        title = f.get("title", "Optimization finding")
        impact = f.get("impact_summary", "")
        sev = f.get("severity", "INFO")

        level = "error" if sev in {"CRITICAL", "HIGH"} else "warning" if sev == "MEDIUM" else "note"

        if rule_id not in rules_map:
            rules_map[rule_id] = {
                "id": rule_id,
                "name": title,
                "shortDescription": {"text": title},
                "fullDescription": {"text": impact},
            }

        start_line = f.get("line_number") or 1
        if isinstance(start_line, str):
            digits = "".join(ch for ch in start_line if ch.isdigit())
            start_line = int(digits) if digits else 1

        results.append(
            {
                "ruleId": rule_id,
                "level": level,
                "message": {"text": f"{title}: {impact}"},
                "locations": [
                    {
                        "physicalLocation": {
                            "artifactLocation": {"uri": fname},
                            "region": {"startLine": start_line},
                        }
                    }
                ],
            }
        )

    return {
        "$schema": "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/sarif-schema-2.1.0.json",
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "dlt-doctor",
                        "version": "0.5.0",
                        "rules": list(rules_map.values()),
                    }
                },
                "results": results,
            }
        ],
    }


def run_action() -> int:
    """Run action pipeline: single analyze execution, formatting, and exit evaluation."""
    path = os.environ.get("INPUT_PATH", "src")
    fmt = os.environ.get("INPUT_FORMAT", "text").lower()
    fail_on = os.environ.get("INPUT_FAIL_ON", "CRITICAL").upper()
    config = os.environ.get("INPUT_CONFIG", "")
    sarif_output = os.environ.get("INPUT_SARIF_OUTPUT", "")
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY", "")

    cmd = [
        sys.executable,
        "-m",
        "dlt_doctor.cli",
        "analyze",
        path,
        "--format",
        "json",
        "--fail-on",
        "NONE",
    ]
    if config:
        cmd.extend(["--config", config])

    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)  # nosec B603
    if proc.returncode != 0 and not proc.stdout:
        sys.stderr.write(proc.stderr)
        return proc.returncode

    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        sys.stderr.write(f"Failed to parse dlt-doctor output: {exc}\n{proc.stdout}\n{proc.stderr}")
        return 1

    # 1. SARIF output if requested
    if sarif_output:
        sarif_data = derive_sarif(data)
        sarif_path = Path(sarif_output)
        sarif_path.parent.mkdir(parents=True, exist_ok=True)
        sarif_path.write_text(json.dumps(sarif_data, indent=2), encoding="utf-8")

    # 2. GitHub Step Summary if available
    summary_md = derive_summary_markdown(data)
    if step_summary:
        try:
            with open(step_summary, "a", encoding="utf-8") as f:
                f.write(summary_md + "\n")
        except OSError as exc:
            sys.stderr.write(f"Warning: could not write to GITHUB_STEP_SUMMARY: {exc}\n")

    # 3. Output to stdout based on requested format
    if fmt == "json":
        print(json.dumps(data, indent=2))
    elif fmt == "sarif":
        print(json.dumps(derive_sarif(data), indent=2))
    elif fmt == "markdown":
        print(summary_md)
    else:
        # text format
        if "file_reports" in data:
            print(f"DLT-Doctor Audit: {data.get('root_path', '.')}")
            print(f"Health Score: {data.get('overall_health_score', 100)}/100")
            print(f"Total Files: {data.get('total_files_scanned', 0)}")
            print(
                f"Clean: {data.get('clean_files_count', 0)}, Flagged: {data.get('flagged_files_count', 0)}"
            )
            for rep in data.get("file_reports", []):
                for f in rep.get("findings", []):
                    print(
                        f"[{f.get('severity')}] {f.get('title')} ({f.get('rule_id')}) in {rep.get('pipeline_name')}"
                    )
        else:
            print(f"DLT-Doctor Audit: {data.get('pipeline_name')}")
            print(f"Health Score: {data.get('overall_health_score', 100)}/100")
            for f in data.get("findings", []):
                print(f"[{f.get('severity')}] {f.get('title')} ({f.get('rule_id')})")

    # 4. Check failure threshold
    if fail_on == "NONE":
        return 0

    threshold = SEVERITY_RANK.get(fail_on, 5)

    all_severities: list[str] = []
    if "file_reports" in data:
        for rep in data.get("file_reports", []):
            for f in rep.get("findings", []):
                all_severities.append(f.get("severity", "INFO"))
    else:
        for f in data.get("findings", []):
            all_severities.append(f.get("severity", "INFO"))

    for s in all_severities:
        if SEVERITY_RANK.get(s, 0) >= threshold:
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(run_action())
