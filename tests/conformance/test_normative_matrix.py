"""Examples and invariants are taken from the independent V1 contract document."""

import copy
from pathlib import Path

import pytest
import yaml

from landfall.core.contract import ContractError, IngestionContract

ROOT = Path(__file__).parents[2] / "ingestion-contract"
EXAMPLES = {
    "01_sqlserver_full_replace.yml": ("sqlserver", "full", "replace"),
    "02_sqlserver_incremental_append_fingerprint.yml": ("sqlserver", "incremental", "append"),
    "03_mongodb_nested_full_replace.yml": ("mongodb", "full", "replace"),
    "04_kafka_microbatch_append.yml": ("kafka", "incremental", "append"),
    "05_object_storage_parquet_incremental.yml": ("object-storage", "incremental", "append"),
    "06_ftp_csv_incremental.yml": ("ftp", "incremental", "append"),
    "07_http_api_watermark.yml": ("http-api", "incremental", "append"),
    "08_sharepoint_list_incremental.yml": ("sharepoint", "incremental", "append"),
    "09_kafka_streaming_append.yml": ("kafka", "continuous", "append"),
}


@pytest.mark.parametrize("filename,expected", EXAMPLES.items())
def test_normative_examples_preserve_language(filename, expected):
    document = yaml.safe_load((ROOT / filename).read_text())
    contract = IngestionContract.parse(document)
    assert (contract.document["source"]["connector"], contract.document["read"]["mode"],
            contract.document["target"]["write"]["mode"]) == expected
    assert contract.document["apiVersion"] == "ingestion.contract/v1alpha1"
    assert contract.document["kind"] == "IngestionContract"


@pytest.fixture
def progressive():
    return yaml.safe_load((ROOT / "02_sqlserver_incremental_append_fingerprint.yml").read_text())


@pytest.mark.parametrize("path,value", [
    (("metadata", "version"), None),
    (("read", "initialPosition"), None),
    (("read", "initialPosition"), "latest"),
    (("read", "position"), None),
    (("read", "mode"), "filters"),
    (("target", "asset"), "Customer_v2"),
])
def test_normative_required_and_semantic_values(progressive, path, value):
    document = copy.deepcopy(progressive)
    target = document
    for component in path[:-1]:
        target = target[component]
    if value is None:
        target.pop(path[-1], None)
    else:
        target[path[-1]] = value
    with pytest.raises(ContractError):
        IngestionContract.parse(document)


def test_full_cannot_carry_position_and_progressive_cannot_replace(progressive):
    full = copy.deepcopy(progressive)
    full["read"]["mode"] = "full"
    with pytest.raises(ContractError):
        IngestionContract.parse(full)
    progressive["target"]["write"]["mode"] = "replace"
    with pytest.raises(ContractError):
        IngestionContract.parse(progressive)


@pytest.mark.parametrize("physical_key", ["storageAccount", "container", "batchSize",
                                          "partitionCount", "runtime", "externalLocation"])
def test_physical_settings_do_not_enter_contract(progressive, physical_key):
    progressive[physical_key] = "anything"
    with pytest.raises(ContractError):
        IngestionContract.parse(progressive)


def test_fingerprint_must_leave_at_least_one_contracted_field(progressive):
    names = [field["name"] for field in progressive["fields"]]
    progressive["target"]["write"]["deduplication"]["excludeFields"] = names
    with pytest.raises(ContractError):
        IngestionContract.parse(progressive)
