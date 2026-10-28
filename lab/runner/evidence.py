"""Structured certification evidence written from scenario results, never logs."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring


class EvidenceReport:
    def __init__(self):
        self.run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
        root = Path(os.environ.get("LANDFALL_ARTIFACTS_DIR", "/app/artifacts/test-runs"))
        self.directory = root / self.run_id
        self.directory.mkdir(parents=True, exist_ok=True)
        (self.directory / "logs").mkdir(exist_ok=True)
        self.scenarios: list[dict] = []

    def add(self, scenario: str, status: str = "PASSED", **evidence) -> None:
        self.scenarios.append({"scenario": scenario, "status": status, **evidence})

    def log(self, name: str, content: str) -> None:
        path = self.directory / "logs" / f"{name}.log"
        path.write_text(content)

    def write(self, status: str, failure: str | None = None) -> Path:
        payload = {
            "runId": self.run_id,
            "generatedAt": datetime.now(UTC).isoformat(),
            "scale": os.environ.get("LANDFALL_TEST_SCALE", "ci"),
            "status": status,
            "scenarioCount": len(self.scenarios),
            "scenarioPasses": sum(item["status"] == "PASSED" for item in self.scenarios),
            "scenarioFailures": sum(item["status"] != "PASSED" for item in self.scenarios),
            "failure": failure,
            "scenarios": self.scenarios,
        }
        (self.directory / "report.json").write_text(json.dumps(payload, indent=2, sort_keys=True))
        (self.directory / "scenarios.json").write_text(
            json.dumps(self.scenarios, indent=2, sort_keys=True))
        (self.directory / "summary.md").write_text(self._summary(payload))
        suite = Element("testsuite", {
            "name": "landfall-lab-certification",
            "tests": str(max(1, len(self.scenarios))),
            "failures": str(payload["scenarioFailures"] + (1 if failure and not self.scenarios else 0)),
            "skipped": "0",
            "time": str(sum(float(item.get("durationSeconds", 0)) for item in self.scenarios)),
        })
        for item in self.scenarios:
            case = SubElement(suite, "testcase", {
                "classname": str(item.get("runtime", {}).get("type", "lab")),
                "name": item["scenario"],
                "time": str(item.get("durationSeconds", 0)),
            })
            if item["status"] != "PASSED":
                SubElement(case, "failure", {"message": item.get("error", "scenario failed")})
        if failure and not self.scenarios:
            case = SubElement(suite, "testcase", {"classname": "lab", "name": "certification"})
            SubElement(case, "failure", {"message": failure})
        (self.directory / "pytest.xml").write_bytes(tostring(suite, encoding="utf-8", xml_declaration=True))
        (self.directory.parent / "latest.json").write_text(json.dumps({"runId": self.run_id}))
        print(f"Certification report: {self.directory / 'summary.md'}")
        return self.directory

    def _summary(self, payload: dict) -> str:
        lines = [
            "# Landfall Certification",
            "",
            f"- Status: **{payload['status']}**",
            f"- Scale: `{payload['scale']}`",
            f"- Scenarios: {payload['scenarioPasses']} passed, {payload['scenarioFailures']} failed",
            "",
            "| Scenario | Runtime | Captured rows | Partitions | Files | Lost | Duplicates |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
        for item in self.scenarios:
            runtime = item.get("runtime", {})
            execution = item.get("execution", {})
            correctness = item.get("correctness", {})
            lines.append(
                f"| {item['scenario']} | {runtime.get('type', 'n/a')}/{runtime.get('mode', 'n/a')} "
                f"| {execution.get('capturedRows', 'n/a')} | "
                f"{execution.get('logicalPartitions', 'n/a')} | {execution.get('files', 'n/a')} | "
                f"{correctness.get('lostRows', 'n/a')} | "
                f"{correctness.get('unexpectedDuplicates', 'n/a')} |"
            )
        if payload.get("failure"):
            lines.extend(("", "## Failure", "", f"```text\n{payload['failure']}\n```"))
        lines.extend(("", "Artifacts: `report.json`, `scenarios.json`, `pytest.xml`, and `logs/`."))
        return "\n".join(lines) + "\n"
