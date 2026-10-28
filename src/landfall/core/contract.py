"""The normative Ingestion Contract V1 language consumed by Landfall.

This module validates the supplied language; it does not add physical configuration.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


class ContractError(ValueError):
    """Invalid Ingestion Contract."""


_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_FIELD = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CONNECTOR = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")
_ASSET = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")
_SCALAR = {"boolean", "int8", "int16", "int32", "int64", "float32", "float64", "string", "binary", "date", "timestamp"}
_OPS = {"eq", "neq", "gt", "gte", "lt", "lte", "in", "not_in", "is_null", "is_not_null"}
_FORBIDDEN_CONTRACT_KEYS = {"storageAccount", "container", "landingContainer", "partitionCount", "workers", "runtime", "batchSize", "fetchSize", "checkpoint", "runId", "externalLocation"}


def fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _require_map(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(f"{path} must be an object")
    return value


def _keys(value: Mapping[str, Any], path: str, required: set[str], optional: set[str] = frozenset()) -> None:
    missing = required - value.keys()
    unknown = value.keys() - required - optional
    if missing:
        raise ContractError(f"{path} missing: {', '.join(sorted(missing))}")
    if unknown:
        raise ContractError(f"{path} unknown: {', '.join(sorted(unknown))}")


def _name(value: Any, path: str, asset: bool = False, field: bool = False) -> str:
    pattern = _ASSET if asset else _FIELD if field else _NAME
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ContractError(f"{path} has an invalid ASCII identifier")
    return value


def _split_generic(text: str) -> list[str]:
    depth, start, parts = 0, 0, []
    for i, ch in enumerate(text):
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
            if depth < 0:
                raise ContractError("malformed composite type")
        elif ch == "," and depth == 0:
            parts.append(text[start:i].strip())
            start = i + 1
    if depth:
        raise ContractError("malformed composite type")
    parts.append(text[start:].strip())
    return parts


def canonical_type(value: Any) -> str:
    if not isinstance(value, str):
        raise ContractError("field type must be a string")
    typ = value.strip()
    if typ in _SCALAR:
        return typ
    dec = re.fullmatch(r"decimal\((\d+),(\d+)\)", typ)
    if dec:
        precision, scale = map(int, dec.groups())
        if not 1 <= precision <= 76 or not 0 <= scale <= precision:
            raise ContractError(f"invalid decimal type: {typ}")
        return typ
    for kind in ("array", "map", "struct"):
        if typ.startswith(kind + "<") and typ.endswith(">"):
            args = _split_generic(typ[len(kind) + 1 : -1])
            if kind == "array" and len(args) == 1:
                canonical_type(args[0])
            elif kind == "map" and len(args) == 2:
                canonical_type(args[0]); canonical_type(args[1])
            elif kind == "struct" and args:
                names = []
                for arg in args:
                    if ":" not in arg:
                        raise ContractError(f"invalid struct member: {arg}")
                    name, member_type = arg.split(":", 1)
                    names.append(_name(name.strip(), "struct member", field=True))
                    canonical_type(member_type.strip())
                if len(names) != len(set(names)):
                    raise ContractError("duplicate struct member")
            else:
                raise ContractError(f"invalid composite type: {typ}")
            return typ
    raise ContractError(f"unsupported canonical type: {typ}")


def _field_type(field: Mapping[str, Any], path: str) -> str:
    typ = field["type"]
    if typ == "struct":
        children = field.get("fields")
        if not isinstance(children, list) or not children:
            raise ContractError(f"{path}.fields required for struct")
        names, members = [], []
        for i, child in enumerate(children):
            child = _require_map(child, f"{path}.fields[{i}]")
            _keys(child, f"{path}.fields[{i}]", {"name", "type"}, {"fields"})
            name = _name(child["name"], f"{path}.fields[{i}].name", field=True)
            names.append(name)
            members.append(name + ":" + _field_type(child, f"{path}.fields[{i}]") )
        if len(names) != len(set(names)):
            raise ContractError(f"{path} has duplicate struct members")
        return "struct<" + ",".join(members) + ">"
    if "fields" in field:
        raise ContractError(f"{path}.fields only valid for struct")
    return canonical_type(typ)


def validate_filter(node: Any, path: str = "read.filters") -> None:
    obj = _require_map(node, path)
    if set(obj) in ({"and"}, {"or"}):
        op, children = next(iter(obj.items()))
        if not isinstance(children, list) or not children:
            raise ContractError(f"{path}.{op} must be a nonempty list")
        for i, child in enumerate(children):
            validate_filter(child, f"{path}.{op}[{i}]")
        return
    _keys(obj, path, {"field", "operator"}, {"value", "values"})
    _name(obj["field"], path + ".field", field=True)
    op = obj["operator"]
    if op not in _OPS:
        raise ContractError(f"{path}.operator invalid: {op}")
    if op in {"is_null", "is_not_null"}:
        if "value" in obj or "values" in obj:
            raise ContractError(f"{path}.{op} takes no value")
    elif op in {"in", "not_in"}:
        if "value" in obj or not isinstance(obj.get("values"), list) or not obj["values"]:
            raise ContractError(f"{path}.{op} requires nonempty values list")
    elif "value" not in obj or "values" in obj:
        raise ContractError(f"{path}.{op} requires one value")


@dataclass(frozen=True)
class IngestionContract:
    document: Mapping[str, Any]

    @classmethod
    def parse(cls, document: Any) -> IngestionContract:
        root = _require_map(document, "contract")
        _keys(root, "contract", {"apiVersion", "kind", "metadata", "source", "fields", "read", "target"}, {"serviceLevel", "execution"})
        if root["apiVersion"] != "ingestion.contract/v1alpha1" or root["kind"] != "IngestionContract":
            raise ContractError("unsupported apiVersion or kind")
        if _FORBIDDEN_CONTRACT_KEYS & root.keys():
            raise ContractError("physical configuration belongs in Platform Config")
        metadata = _require_map(root["metadata"], "metadata")
        _keys(metadata, "metadata", {"version", "domain", "sourceSystem", "owner"}, {"costAllocation", "tags"})
        if not isinstance(metadata["version"], str) or not metadata["version"].strip():
            raise ContractError("metadata.version must be a nonempty version string")
        for key in ("domain", "sourceSystem"):
            _name(metadata[key], "metadata." + key)
        if not isinstance(metadata["owner"], str) or not metadata["owner"].strip():
            raise ContractError("metadata.owner is required")
        if "tags" in metadata and not isinstance(metadata["tags"], dict):
            raise ContractError("metadata.tags must be an object")
        source = _require_map(root["source"], "source")
        _keys(source, "source", {"connector", "connectionRef", "resource"})
        if not isinstance(source["connector"], str) or not _CONNECTOR.fullmatch(source["connector"]):
            raise ContractError("source.connector has invalid syntax")
        if not isinstance(source["connectionRef"], str) or not source["connectionRef"].strip():
            raise ContractError("source.connectionRef is required")
        _require_map(source["resource"], "source.resource")
        fields = root["fields"]
        if not isinstance(fields, list) or not fields:
            raise ContractError("fields must be a nonempty allowlist")
        names = []
        for i, field in enumerate(fields):
            field = _require_map(field, f"fields[{i}]")
            _keys(field, f"fields[{i}]", {"name", "type"}, {"fields"})
            names.append(_name(field["name"], f"fields[{i}].name", field=True))
            _field_type(field, f"fields[{i}]")
        if len(names) != len(set(names)):
            raise ContractError("duplicate fields")
        read = _require_map(root["read"], "read")
        _keys(read, "read", {"mode"}, {"initialPosition", "position", "filters"})
        if read["mode"] not in {"full", "incremental", "continuous"}:
            raise ContractError("read.mode must be full, incremental or continuous")
        if "filters" in read:
            validate_filter(read["filters"])
        if read["mode"] in {"incremental", "continuous"}:
            if "initialPosition" not in read or "position" not in read:
                raise ContractError("progressive reads require initialPosition and position")
            if read["initialPosition"] not in {"beginning", "current"}:
                raise ContractError("initialPosition must be beginning or current")
            position = _require_map(read["position"], "read.position")
            _keys(position, "read.position", {"strategy"}, {"fields"})
            if position["strategy"] not in {"ordered_fields", "source_native"}:
                raise ContractError("invalid SourcePosition strategy")
            if position["strategy"] == "ordered_fields":
                pfields = position.get("fields")
                if not isinstance(pfields, list) or not pfields:
                    raise ContractError("ordered_fields requires fields")
                for name in pfields:
                    _name(name, "position field", field=True)
                if len(set(pfields)) != len(pfields):
                    raise ContractError("duplicate position fields")
                if not set(pfields) <= set(names):
                    raise ContractError("position fields must resolve in contracted fields")
            elif "fields" in position:
                raise ContractError("source_native has no ordered fields")
        elif "initialPosition" in read or "position" in read:
            raise ContractError("full mode cannot specify position or initialPosition")
        target = _require_map(root["target"], "target")
        _keys(target, "target", {"asset", "write"})
        _name(target["asset"], "target.asset", asset=True)
        write = _require_map(target["write"], "target.write")
        _keys(write, "target.write", {"mode"}, {"deduplication"})
        if write["mode"] not in {"append", "replace"}:
            raise ContractError("target.write.mode must be append or replace")
        if read["mode"] in {"incremental", "continuous"} and write["mode"] == "replace":
            raise ContractError("progressive read + replace is invalid V1")
        if "deduplication" in write:
            dedup = _require_map(write["deduplication"], "target.write.deduplication")
            _keys(dedup, "target.write.deduplication", {"strategy"}, {"excludeFields"})
            if write["mode"] != "append" or dedup["strategy"] != "fingerprint":
                raise ContractError("fingerprint requires append")
            excludes = dedup.get("excludeFields", [])
            if not isinstance(excludes, list) or len(set(excludes)) != len(excludes) or not set(excludes) <= set(names) or len(excludes) >= len(names):
                raise ContractError("invalid fingerprint excludeFields")
        if "serviceLevel" in root:
            sl = _require_map(root["serviceLevel"], "serviceLevel")
            _keys(sl, "serviceLevel", set(), {"targetDuration"})
            if read["mode"] == "continuous" and "targetDuration" in sl:
                raise ContractError("continuous cannot declare targetDuration")
            if "targetDuration" in sl and not re.fullmatch(r"PT(?=\d+[HMS])(?:\d+H)?(?:\d+M)?(?:\d+S)?", str(sl["targetDuration"])):
                raise ContractError("serviceLevel.targetDuration must be ISO 8601 duration")
        if "execution" in root:
            execution = _require_map(root["execution"], "execution")
            _keys(execution, "execution", set(), {"constraints"})
            if "constraints" in execution:
                constraints = _require_map(execution["constraints"], "execution.constraints")
                _keys(constraints, "execution.constraints", set(), {"maxSourceConnections"})
                val = constraints.get("maxSourceConnections")
                if val is not None and (type(val) is not int or val < 1):
                    raise ContractError("maxSourceConnections must be positive integer")
        return cls(root)

    @property
    def identity(self) -> str:
        d = self.document
        return ".".join((d["metadata"]["domain"], d["metadata"]["sourceSystem"], d["target"]["asset"]))

    @property
    def version(self) -> str:
        return self.document["metadata"]["version"]

    @property
    def hash(self) -> str:
        return fingerprint(self.document)

    @property
    def fields(self) -> tuple[tuple[str, str], ...]:
        return tuple((f["name"], _field_type(f, f"fields[{i}]")) for i, f in enumerate(self.document["fields"]))
