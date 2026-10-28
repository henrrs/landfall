"""SQL Server connector: metadata, position, profiling, predicates, Arrow reading."""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa

from landfall.core.contract import ContractError, IngestionContract, fingerprint
from landfall.core.models import (
    ColumnProfile,
    Partition,
    SemanticReadScope,
    SourceCapabilities,
    SourcePosition,
    SourceProfile,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_NUMERIC = {"tinyint": "int16", "smallint": "int16", "int": "int32", "bigint": "int64",
            "real": "float32", "float": "float64", "bit": "boolean"}
_STRING = {"char", "varchar", "nchar", "nvarchar", "text", "ntext", "xml", "uniqueidentifier"}
_BINARY = {"binary", "varbinary", "image", "timestamp", "rowversion"}
_TEMPORAL = {"datetime", "datetime2", "smalldatetime", "datetimeoffset"}


def quoted(identifier: str) -> str:
    if not _IDENTIFIER.fullmatch(identifier):
        raise ContractError(f"invalid SQL Server identifier: {identifier!r}")
    return f"[{identifier}]"


def sql_type_to_canonical(sql_type: str, precision: int | None = None,
                          scale: int | None = None) -> str:
    name = sql_type.lower()
    if name in _NUMERIC:
        return _NUMERIC[name]
    if name in _STRING:
        return "string"
    if name in _BINARY:
        return "binary"
    if name in _TEMPORAL:
        return "timestamp"
    if name == "date":
        return "date"
    if name in {"decimal", "numeric"}:
        return f"decimal({precision},{scale})"
    raise ContractError(f"SQL Server type {sql_type} has no safe V1 canonical mapping")


def _position_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _lexicographic(fields: tuple[str, ...], values: tuple[Any, ...], upper: bool) -> tuple[str, list[Any]]:
    if len(fields) != len(values):
        raise ValueError("SourcePosition width does not match position fields")
    terms, params = [], []
    for i, field in enumerate(fields):
        prefix = " AND ".join(f"{quoted(fields[j])} = ?" for j in range(i))
        comparator = "<" if upper else ">"
        terms.append("(" + (prefix + " AND " if prefix else "") + f"{quoted(field)} {comparator} ?)")
        params.extend(values[:i])
        params.append(values[i])
    if upper:
        terms.append("(" + " AND ".join(f"{quoted(field)} = ?" for field in fields) + ")")
        params.extend(values)
    return "(" + " OR ".join(terms) + ")", params


def compile_filter(node: dict[str, Any]) -> tuple[str, list[Any]]:
    if "and" in node or "or" in node:
        op = "and" if "and" in node else "or"
        compiled = [compile_filter(child) for child in node[op]]
        return "(" + (" AND " if op == "and" else " OR ").join(sql for sql, _ in compiled) + ")", [p for _, params in compiled for p in params]
    field, op = quoted(node["field"]), node["operator"]
    if op == "is_null":
        return f"{field} IS NULL", []
    if op == "is_not_null":
        return f"{field} IS NOT NULL", []
    if op in {"in", "not_in"}:
        values = node["values"]
        if any(v is None for v in values):
            raise ContractError("NULL in filter list is ambiguous; use is_null")
        sql_op = "IN" if op == "in" else "NOT IN"
        return f"{field} {sql_op} (" + ", ".join("?" for _ in values) + ")", list(values)
    value = node["value"]
    if value is None:
        raise ContractError("NULL comparison is ambiguous; use is_null/is_not_null")
    symbol = {"eq": "=", "neq": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[op]
    return f"{field} {symbol} ?", [value]


class SqlServerConnector:
    capabilities = SourceCapabilities()

    def __init__(self, secret: dict[str, Any]):
        if "connectionString" in secret:
            self.connection_string = secret["connectionString"]
        else:
            required = {"server", "database", "user", "password"}
            if not required <= secret.keys():
                raise ValueError("SQL Server secret missing server, database, user or password")
            server = secret["server"]
            if secret.get("port"):
                server += "," + str(secret["port"])
            self.connection_string = (
                f"Server={server};Database={secret['database']};UID={secret['user']};"
                f"PWD={secret['password']};Encrypt={secret.get('encrypt', 'yes')};"
                f"TrustServerCertificate={secret.get('trustServerCertificate', 'no')}"
            )

    def _connect(self):
        import mssql_python

        return mssql_python.connect(self.connection_string)

    @staticmethod
    def _resource(contract: IngestionContract) -> tuple[str, str, str]:
        resource = contract.document["source"]["resource"]
        if set(resource) != {"database", "schema", "table"}:
            raise ContractError("SQL Server resource requires exactly database, schema, table")
        return tuple(quoted(resource[key]) for key in ("database", "schema", "table"))

    @classmethod
    def _table(cls, contract: IngestionContract) -> str:
        return ".".join(cls._resource(contract))

    def _metadata(self, contract: IngestionContract) -> tuple[dict[str, dict], tuple[str, ...]]:
        db, _, _ = self._resource(contract)
        resource = contract.document["source"]["resource"]
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE "
                        f"FROM {db}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
                        (resource["schema"], resource["table"]))
            columns = {row[0].lower(): {"name": row[0], "type": row[1], "precision": row[2],
                                        "scale": row[3], "nullable": row[4] == "YES"} for row in cur.fetchall()}
            if not columns:
                raise ContractError("SQL Server source table not found")
            cur.execute(f"SELECT k.COLUMN_NAME FROM {db}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS t "
                        f"JOIN {db}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE k ON "
                        "t.CONSTRAINT_NAME=k.CONSTRAINT_NAME AND t.TABLE_SCHEMA=k.TABLE_SCHEMA "
                        "AND t.TABLE_NAME=k.TABLE_NAME "
                        "WHERE t.CONSTRAINT_TYPE='PRIMARY KEY' AND t.TABLE_SCHEMA=? AND t.TABLE_NAME=? "
                        "ORDER BY k.ORDINAL_POSITION", (resource["schema"], resource["table"]))
            pk = tuple(row[0].lower() for row in cur.fetchall())
            return columns, pk
        finally:
            conn.close()

    def validate(self, contract: IngestionContract) -> None:
        if contract.document["source"]["connector"] != "sqlserver":
            raise ContractError("SQL Server connector mismatch")
        if contract.document["read"]["mode"] == "continuous":
            raise ContractError("SQL Server connector V1 does not support continuous read")
        columns, pk = self._metadata(contract)
        for name, target_type in contract.fields:
            if name.lower() not in columns:
                raise ContractError(f"contracted field missing in source: {name}")
            source = columns[name.lower()]
            physical_type = sql_type_to_canonical(source["type"], source["precision"], source["scale"])
            if physical_type != target_type:
                raise ContractError(f"field {name}: SQL Server {source['type']} maps to {physical_type}, not {target_type}")
        read = contract.document["read"]
        if read["mode"] == "incremental":
            position = read["position"]
            if position["strategy"] != "ordered_fields":
                raise ContractError("SQL Server V1 supports ordered_fields SourcePosition")
            pfields = tuple(position["fields"])
            for field in pfields:
                if field.lower() not in columns:
                    raise ContractError(f"position field missing: {field}")
                if columns[field.lower()]["nullable"]:
                    raise ContractError(f"position field must be non-nullable: {field}")
                typ = sql_type_to_canonical(columns[field.lower()]["type"], columns[field.lower()]["precision"], columns[field.lower()]["scale"])
                if typ in {"binary", "boolean"}:
                    raise ContractError(f"position field not safely orderable: {field}")
            if not pk or not set(pk) <= {f.lower() for f in pfields}:
                raise ContractError("ordered_fields must include the source primary key for deterministic progress")
        filters = read.get("filters")
        if filters:
            self._validate_filter_fields(filters, columns)
            compile_filter(filters)

    def _validate_filter_fields(self, node: dict, columns: dict) -> None:
        if "and" in node or "or" in node:
            for child in node.get("and", node.get("or", [])):
                self._validate_filter_fields(child, columns)
        elif node["field"].lower() not in columns:
            raise ContractError(f"filter field missing: {node['field']}")

    def high_position(self, contract: IngestionContract) -> SourcePosition | None:
        read = contract.document["read"]
        fields = tuple(read["position"]["fields"])
        clauses = [f"{quoted(f)} IS NOT NULL" for f in fields]
        params: list[Any] = []
        if read.get("filters"):
            sql, extra = compile_filter(read["filters"])
            clauses.append(sql)
            params.extend(extra)
        projection = ", ".join(quoted(f) for f in fields)
        order = ", ".join(quoted(f) + " DESC" for f in fields)
        query = f"SELECT TOP 1 {projection} FROM {self._table(contract)} WHERE " + " AND ".join(clauses) + " ORDER BY " + order
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(query, tuple(params))
            row = cur.fetchone()
            return SourcePosition("ordered_fields", tuple(_position_value(v) for v in row)) if row else None
        finally:
            conn.close()

    def profile(self, contract: IngestionContract) -> SourceProfile:
        columns, pk = self._metadata(contract)
        db, _, _ = self._resource(contract)
        resource = contract.document["source"]["resource"]
        conn = self._connect()
        try:
            cur = conn.cursor()
            qualified = f"{db}.{quoted(resource['schema'])}.{quoted(resource['table'])}"
            cur.execute(f"SELECT COALESCE(SUM([rows]),0) FROM {db}.sys.partitions "
                        "WHERE object_id=OBJECT_ID(?) AND index_id IN (0,1)", (qualified,))
            estimated_rows = int(cur.fetchone()[0] or 0)
            profiles = []
            for name, col in columns.items():
                try:
                    typ = sql_type_to_canonical(col["type"], col["precision"], col["scale"])
                except ContractError:
                    continue
                minimum = maximum = None
                if name in pk and (typ.startswith("int") or typ in {"date", "timestamp"}):
                    sql_name = quoted(col["name"])
                    cur.execute(f"SELECT TOP 1 {sql_name} FROM {qualified} WHERE {sql_name} IS NOT NULL ORDER BY {sql_name} ASC")
                    row = cur.fetchone()
                    minimum = _position_value(row[0]) if row else None
                    cur.execute(f"SELECT TOP 1 {sql_name} FROM {qualified} WHERE {sql_name} IS NOT NULL ORDER BY {sql_name} DESC")
                    row = cur.fetchone()
                    maximum = _position_value(row[0]) if row else None
                profiles.append(ColumnProfile(name, typ, col["nullable"], minimum, maximum,
                                              indexed=name in pk, unique=len(pk) == 1 and name in pk))
            widths = {"boolean": 1, "int8": 1, "int16": 2, "int32": 4, "int64": 8,
                      "float32": 4, "float64": 8, "date": 4, "timestamp": 8,
                      "binary": 64, "string": 64}
            row_bytes = sum(widths.get(typ, 16) for _, typ in contract.fields)
            schema_hash = fingerprint([(name, columns[name]["type"], columns[name]["precision"],
                                        columns[name]["scale"], columns[name]["nullable"])
                                       for name in sorted(columns)])
            return SourceProfile(uuid.uuid4().hex, estimated_rows, max(1, row_bytes), schema_hash,
                                 tuple(profiles), (pk,) if pk else (), 0.6,
                                 tuple(columns))
        finally:
            conn.close()

    def read(self, scope: SemanticReadScope, partition: Partition,
             batch_size: int) -> Iterator[pa.RecordBatch]:
        # Table identity is cached only for the duration of this connector instance.
        if not hasattr(self, "table"):
            raise RuntimeError("bind contract before reading")
        projection = ", ".join(f"{quoted(name)} AS {quoted(name)}" for name, _ in scope.fields)
        clauses, params = [], []
        if scope.filters:
            sql, values = compile_filter(scope.filters)
            clauses.append(sql)
            params.extend(values)
        if scope.mode == "incremental":
            if scope.high is None:
                clauses.append("1=0")
            else:
                if scope.low:
                    sql, values = _lexicographic(scope.position_fields, scope.low.values, False)
                    clauses.append(sql)
                    params.extend(values)
                sql, values = _lexicographic(scope.position_fields, scope.high.values, True)
                clauses.append(sql)
                params.extend(values)
        if partition.kind == "RANGE":
            field = quoted(partition.field)
            if partition.lower is not None:
                clauses.append(f"{field} >= ?")
                params.append(partition.lower)
            if partition.upper is not None:
                clauses.append(f"{field} < ?")
                params.append(partition.upper)
        elif partition.kind == "NULL":
            clauses.append(f"{quoted(partition.field)} IS NULL")
        query = f"SELECT {projection} FROM {self.table}"
        if clauses:
            query += " WHERE " + " AND ".join(f"({clause})" for clause in clauses)
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(query, tuple(params))
            yield from cur.arrow_reader(batch_size=batch_size)
        finally:
            conn.close()

    def bind(self, contract: IngestionContract) -> SqlServerConnector:
        self.table = self._table(contract)
        return self
