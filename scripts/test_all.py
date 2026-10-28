"""Run the reproducible certification gates and retain machine-readable evidence."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_ROOT = Path(os.environ.get("LANDFALL_ARTIFACT_ROOT", ROOT / "build" / "test-runs"))


def _run_step(name: str, command: list[str], directory: Path, env: dict[str, str]) -> dict:
    started = time.monotonic()
    completed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True,
                               text=True, check=False)
    elapsed = time.monotonic() - started
    log = directory / "logs" / f"{name}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(completed.stdout + "\n--- stderr ---\n" + completed.stderr)
    return {"name": name, "command": command, "status": "PASSED" if completed.returncode == 0 else "FAILED",
            "exitCode": completed.returncode, "durationSeconds": elapsed,
            "log": str(log.relative_to(directory))}


def _pytest_counts(path: Path) -> dict[str, int]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    result = {"tests": 0, "passes": 0, "failures": 0, "errors": 0, "skipped": 0}
    for suite in suites:
        result["tests"] += int(suite.get("tests", 0))
        result["failures"] += int(suite.get("failures", 0))
        result["errors"] += int(suite.get("errors", 0))
        result["skipped"] += int(suite.get("skipped", 0))
    result["passes"] = result["tests"] - result["failures"] - result["errors"] - result["skipped"]
    return result


def _latest_lab_report() -> dict | None:
    pointer = ARTIFACT_ROOT / "latest.json"
    if not pointer.is_file():
        return None
    run_id = json.loads(pointer.read_text())["runId"]
    path = ARTIFACT_ROOT / run_id / "report.json"
    return json.loads(path.read_text()) if path.is_file() else None


def _gate(runtime: str, scale: str, directory: Path) -> tuple[list[dict], list[dict], dict | None]:
    env = dict(os.environ)
    env["LANDFALL_TEST_SCALE"] = scale
    env["PYTHONPATH"] = "src"
    steps = []
    junit_files = []
    lab_report = None

    if runtime in {"nospark", "all"}:
        lock_xml = directory / "contract-lock.xml"
        step = _run_step("contract-artifact-integrity", [sys.executable, "-m", "pytest", "-q",
            "tests/conformance/test_contract_integrity.py", f"--junitxml={lock_xml}"], directory, env)
        steps.append(step)
        junit_files.append(lock_xml)
        steps.append(_run_step("lint", ["make", "lint"], directory, env))
        test_xml = directory / "pytest.xml"
        test_command = [sys.executable, "-m", "pytest", "-q", "tests/architecture", "tests/unit",
                        "tests/property", "tests/conformance", "tests/integration",
                        f"--junitxml={test_xml}"]
        steps.append(_run_step("pytest-nospark", test_command, directory, env))
        junit_files.append(test_xml)
        steps.append(_run_step("wheel-verify", ["make", "verify-wheel"], directory, env))
        steps.append(_run_step("local-lab-e2e", ["make", "e2e-local"], directory, env))
        lab_report = _latest_lab_report()
        diff_step = _run_step("diff-check", ["git", "diff", "--check"], directory, env)
        steps.append(diff_step)

    if runtime in {"spark", "all"}:
        lock_xml = directory / "spark-contract-lock.xml"
        step = _run_step("spark-contract-artifact-integrity", [sys.executable, "-m", "pytest", "-q",
            "tests/conformance/test_contract_integrity.py", f"--junitxml={lock_xml}"], directory, env)
        steps.append(step)
        junit_files.append(lock_xml)
        spark_xml = directory / "spark-pytest.xml"
        steps.append(_run_step("pytest-spark", [sys.executable, "-m", "pytest", "-q",
            "tests/architecture", "tests/spark", f"--junitxml={spark_xml}"], directory, env))
        junit_files.append(spark_xml)
        steps.append(_run_step("spark-wheel-build", ["make", "build"], directory, env))
        steps.append(_run_step("spark-single-and-multi-e2e", ["make", "e2e-spark-all"], directory, env))
        lab_report = _latest_lab_report()
        steps.append(_run_step("spark-diff-check", ["git", "diff", "--check"], directory, env))

    return steps, junit_files, lab_report


def _combine_junit(files: list[Path], destination: Path) -> dict[str, int]:
    suites = ET.Element("testsuites")
    totals = {"tests": 0, "passes": 0, "failures": 0, "errors": 0, "skipped": 0}
    for path in files:
        if not path.is_file():
            continue
        root = ET.parse(path).getroot()
        children = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
        for suite in children:
            suites.append(suite)
            totals["tests"] += int(suite.get("tests", 0))
            totals["failures"] += int(suite.get("failures", 0))
            totals["errors"] += int(suite.get("errors", 0))
            totals["skipped"] += int(suite.get("skipped", 0))
    totals["passes"] = totals["tests"] - totals["failures"] - totals["errors"] - totals["skipped"]
    suites.set("tests", str(totals["tests"]))
    suites.set("failures", str(totals["failures"]))
    suites.set("errors", str(totals["errors"]))
    suites.set("skipped", str(totals["skipped"]))
    destination.write_bytes(ET.tostring(suites, encoding="utf-8", xml_declaration=True))
    return totals


def _write_report(runtime: str, scale: str, directory: Path, steps: list[dict],
                  junit_files: list[Path], lab_report: dict | None) -> int:
    tests = _combine_junit(junit_files, directory / "pytest.xml")
    scenarios = list(lab_report.get("scenarios", [])) if lab_report else []
    failed_steps = [step for step in steps if step["status"] != "PASSED"]
    failed_scenarios = [scenario for scenario in scenarios if scenario.get("status") != "PASSED"]
    status = "PASSED" if not failed_steps and not failed_scenarios else "FAILED"
    payload = {
        "runId": directory.name,
        "generatedAt": datetime.now(UTC).isoformat(),
        "runtimeGate": runtime,
        "scale": scale,
        "status": status,
        "tests": tests,
        "scenarios": scenarios,
        "scenarioCount": len(scenarios),
        "steps": steps,
        "labReport": lab_report.get("runId") if lab_report else None,
    }
    (directory / "report.json").write_text(json.dumps(payload, indent=2, sort_keys=True))
    (directory / "scenarios.json").write_text(json.dumps(scenarios, indent=2, sort_keys=True))
    lines = [
        f"# Landfall {runtime} certification",
        "",
        f"- Status: **{status}**",
        f"- Scale: `{scale}`",
        (f"- Pytest: {tests['passes']} passed, {tests['failures']} failed, "
         f"{tests['errors']} errors, {tests['skipped']} skipped ({tests['tests']} total)"),
        (f"- Scenarios: {sum(s.get('status') == 'PASSED' for s in scenarios)} passed, "
         f"{len(failed_scenarios)} failed ({len(scenarios)} total)"),
        "",
        "## Runtime scenarios",
        "",
        "| Scenario | Runtime | Captured | Partitions | Files | Lost | Duplicates |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for scenario in scenarios:
        runtime_info = scenario.get("runtime", {})
        execution = scenario.get("execution", {})
        correctness = scenario.get("correctness", {})
        lines.append(
            f"| {scenario.get('scenario')} | {runtime_info.get('type')}/{runtime_info.get('mode')} | "
            f"{execution.get('capturedRows')} | {execution.get('logicalPartitions')} | "
            f"{execution.get('files')} | {correctness.get('lostRows')} | "
            f"{correctness.get('unexpectedDuplicates')} |"
        )
    lines.extend(("", "## Gate steps", "", "| Step | Result | Seconds |", "|---|---|---:|"))
    lines.extend(f"| {step['name']} | {step['status']} | {step['durationSeconds']:.2f} |" for step in steps)
    lines.extend(("", "Artifacts: `report.json`, `scenarios.json`, `pytest.xml`, and `logs/`."))
    (directory / "summary.md").write_text("\n".join(lines) + "\n")
    return 0 if status == "PASSED" else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime", choices=("nospark", "spark", "all"), required=True)
    parser.add_argument("--scale", choices=("tiny", "ci", "full"), default="ci")
    args = parser.parse_args()
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
    directory = ARTIFACT_ROOT / f"gate-{args.runtime}-{run_id}"
    directory.mkdir(parents=True, exist_ok=True)
    steps: list[dict] = []
    junit_files: list[Path] = []
    lab_report = None
    try:
        steps, junit_files, lab_report = _gate(args.runtime, args.scale, directory)
    except Exception as exc:  # noqa: BLE001 - always emit a failure report for the gate
        (directory / "logs" / "orchestrator-error.log").write_text(
            f"{type(exc).__name__}: {exc}")
        steps.append({"name": "orchestrator", "status": "FAILED", "exitCode": 1,
                      "durationSeconds": 0, "log": "logs/orchestrator-error.log"})
    result = _write_report(args.runtime, args.scale, directory, steps, junit_files, lab_report)
    print(f"Full certification report: {directory / 'summary.md'}")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
