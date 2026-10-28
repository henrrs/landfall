"""The normative contract artifacts are locked independently of parser behavior."""

from hashlib import sha256
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2] / "ingestion-contract"
EXPECTED_FILES = {
    "contrato_ingestao_v1.md",
    "ingestion_contract.template.yml",
    *(f"{index:02d}_{name}" for index, name in enumerate((
        "sqlserver_full_replace.yml",
        "sqlserver_incremental_append_fingerprint.yml",
        "mongodb_nested_full_replace.yml",
        "kafka_microbatch_append.yml",
        "object_storage_parquet_incremental.yml",
        "ftp_csv_incremental.yml",
        "http_api_watermark.yml",
        "sharepoint_list_incremental.yml",
        "kafka_streaming_append.yml",
    ), 1)),
}
CANONICAL_MARKDOWN_SHA256 = "8b988ca5307b5399411f4276881c1504590d8907534f6577bbfbe83e8413f1f8"


def _locked_artifacts() -> dict[str, str]:
    entries = {}
    for line in (ROOT / "SHA256SUMS").read_text().splitlines():
        digest, filename = line.split(maxsplit=1)
        filename = filename.removeprefix("*").removeprefix("./")
        assert filename not in entries, f"duplicate lock entry: {filename}"
        entries[filename] = digest
    return entries


def test_normative_artifact_integrity_is_locked():
    locked = _locked_artifacts()
    assert set(locked) == EXPECTED_FILES
    assert locked["contrato_ingestao_v1.md"] == CANONICAL_MARKDOWN_SHA256
    for filename, expected in locked.items():
        actual = sha256((ROOT / filename).read_bytes()).hexdigest()
        assert actual == expected, f"normative artifact drift: {filename}"


@pytest.mark.parametrize("filename", sorted(EXPECTED_FILES - {"contrato_ingestao_v1.md"}))
def test_normative_file_is_present_and_nonempty(filename):
    assert (ROOT / filename).is_file()
    assert (ROOT / filename).stat().st_size > 0
