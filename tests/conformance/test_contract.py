import copy
from pathlib import Path

import pytest
import yaml

from landfall.core.contract import ContractError, IngestionContract, canonical_type


@pytest.fixture
def document():
    return {
        "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
        "metadata": {"version": "1.0.0", "domain": "customer", "sourceSystem": "crm", "owner": "data"},
        "source": {"connector": "sqlserver", "connectionRef": "crm-local",
                   "resource": {"database": "CRM", "schema": "dbo", "table": "Customer"}},
        "fields": [{"name": "customer_id", "type": "int64"},
                   {"name": "status", "type": "string"},
                   {"name": "updated_at", "type": "timestamp"}],
        "read": {"mode": "incremental", "initialPosition": "beginning",
                 "position": {"strategy": "ordered_fields", "fields": ["updated_at", "customer_id"]},
                 "filters": {"and": [{"field": "status", "operator": "eq", "value": "ACTIVE"}]}},
        "target": {"asset": "customer", "write": {"mode": "append"}},
        "execution": {"constraints": {"maxSourceConnections": 2}},
    }


def test_acceptance_contract(document):
    contract = IngestionContract.parse(document)
    assert contract.identity == "customer.crm.customer"
    assert len(contract.hash) == 64


@pytest.mark.parametrize("mode,write,valid", [
    ("full", "replace", True), ("full", "append", True),
    ("incremental", "append", True), ("incremental", "replace", False),
    ("continuous", "append", True), ("continuous", "replace", False)])
def test_read_write_matrix(document, mode, write, valid):
    document["read"]["mode"] = mode
    if mode == "full":
        del document["read"]["initialPosition"]
        del document["read"]["position"]
    document["target"]["write"]["mode"] = write
    if valid:
        IngestionContract.parse(document)
    else:
        with pytest.raises(ContractError):
            IngestionContract.parse(document)


def test_fingerprint_constraints(document):
    write = document["target"]["write"]
    write["deduplication"] = {"strategy": "fingerprint", "excludeFields": ["updated_at"]}
    IngestionContract.parse(document)
    write["deduplication"]["excludeFields"] = [f["name"] for f in document["fields"]]
    with pytest.raises(ContractError):
        IngestionContract.parse(document)


@pytest.mark.parametrize("change", [
    lambda d: d["read"].update(mode="filters"),
    lambda d: d["read"].update(initialPosition="latest"),
    lambda d: d["fields"].append(copy.deepcopy(d["fields"][0])),
    lambda d: d.update(storageAccount="secret"),
    lambda d: d["target"].update(asset="Customer-v2"),
    lambda d: d["execution"]["constraints"].update(maxSourceConnections=0),
])
def test_invalid_contracts(document, change):
    change(document)
    with pytest.raises(ContractError):
        IngestionContract.parse(document)


@pytest.mark.parametrize("typ", ["boolean", "int64", "decimal(18,2)", "decimal(39,2)", "array<string>",
                                  "map<string,int32>", "struct<id:int64,tags:array<string>>"])
def test_canonical_types(typ):
    assert canonical_type(typ) == typ


@pytest.mark.parametrize("typ", ["varchar", "decimal(77,2)", "decimal(2,3)", "array<>", "struct<id:foo>"])
def test_invalid_canonical_types(typ):
    with pytest.raises(ContractError):
        canonical_type(typ)


@pytest.mark.parametrize("path", sorted((Path(__file__).parents[2] / "ingestion-contract").glob("0*.yml")))
def test_normative_reference_examples(path):
    contract = IngestionContract.parse(yaml.safe_load(path.read_text()))
    assert contract.version
    assert contract.identity.count(".") == 2


def test_normative_filter_values_and_continuous_rules(document):
    document["read"]["filters"] = {"field": "status", "operator": "in", "values": ["A", "B"]}
    IngestionContract.parse(document)
    document["read"]["filters"]["value"] = ["A"]
    with pytest.raises(ContractError):
        IngestionContract.parse(document)
    del document["read"]["filters"]["value"]
    document["read"]["mode"] = "continuous"
    document["serviceLevel"] = {"targetDuration": "PT30M"}
    with pytest.raises(ContractError):
        IngestionContract.parse(document)


def test_version_format_is_recommended_not_required(document):
    document["metadata"]["version"] = "2026-10"
    assert IngestionContract.parse(document).version == "2026-10"
