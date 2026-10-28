"""SQL Server connector: metadata, position, profiling, predicates, Arrow reading."""

from __future__ import annotations

import logging
import re
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
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
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(UTC).isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def distribution_skew(histogram: tuple[tuple[Any, int], ...], logical_type: str) -> float:
    """Compare observed histogram mass with equal-width physical ranges."""
    if not histogram:
        return 1.0
    counts = [count for _, count in histogram]
    total = sum(counts)
    if not total:
        return 1.0
    skew = max(counts) / max(1, total / len(counts))
    try:
        if logical_type.startswith(("int", "decimal(")):
            values = [float(value) for value, _ in histogram]
        elif logical_type == "date":
            values = [date.fromisoformat(str(value)).toordinal() for value, _ in histogram]
        elif logical_type == "timestamp":
            times = [datetime.fromisoformat(str(value)) for value, _ in histogram]
            origin = min(times)
            values = [(value - origin).total_seconds() for value in times]
        else:
            return skew
        low, high = min(values), max(values)
        if low == high:
            return skew
        count = min(16, len(histogram))
        buckets = [0] * count
        for value, rows in zip(values, counts, strict=True):
            index = min(count - 1, int((value - low) / (high - low) * count))
            buckets[index] += rows
        return max(skew, max(buckets) / max(1, total / count))
    except (ValueError, TypeError, OverflowError):
        return skew


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
    capabilities = SourceCapabilities(low_cardinality=True)

    def __init__(self, secret: dict[str, Any], profiling: dict[str, Any] | None = None):
        self.profiling = profiling or {}
        self.query_timeout_seconds = int(self.profiling.get("queryTimeoutSeconds", 300))
        if self.query_timeout_seconds < 1:
            raise ValueError("queryTimeoutSeconds must be positive")
        self.probe_rows = max(0, min(2048, int(self.profiling.get("probeMaxRows", 256))))
        self.probe_max_bytes = max(1, int(self.profiling.get("probeMaxBytes", 8 * 1024**2)))
        self.probe_timeout_seconds = max(1, int(self.profiling.get("probeTimeoutSeconds", 15)))
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

        conn = mssql_python.connect(self.connection_string)
        conn.timeout = self.query_timeout_seconds
        return conn

    @staticmethod
    def _resource(contract: IngestionContract) -> tuple[str, str, str]:
        resource = contract.document["source"]["resource"]
        if set(resource) != {"database", "schema", "table"}:
            raise ContractError("SQL Server resource requires exactly database, schema, table")
        return tuple(quoted(resource[key]) for key in ("database", "schema", "table"))

    @classmethod
    def _table(cls, contract: IngestionContract) -> str:
        return ".".join(cls._resource(contract))

    def _metadata(self, contract: IngestionContract) -> tuple[dict[str, dict], tuple[str, ...], tuple[tuple[str, ...], ...]]:
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
            qualified = f"{db}.{quoted(resource['schema'])}.{quoted(resource['table'])}"
            cur.execute(f"SELECT i.index_id,ic.key_ordinal,c.name FROM {db}.sys.indexes i "
                        f"JOIN {db}.sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id "
                        f"JOIN {db}.sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id "
                        "WHERE i.object_id=OBJECT_ID(?) AND i.is_unique=1 AND i.has_filter=0 "
                        "AND ic.key_ordinal>0 ORDER BY i.index_id,ic.key_ordinal", (qualified,))
            by_index = {}
            for index_id, _, name in cur.fetchall():
                by_index.setdefault(index_id, []).append(str(name).lower())
            unique_keys = tuple(tuple(names) for names in by_index.values())
            return columns, pk, unique_keys
        finally:
            conn.close()

    def validate(self, contract: IngestionContract) -> None:
        if contract.document["source"]["connector"] != "sqlserver":
            raise ContractError("SQL Server connector mismatch")
        if contract.document["read"]["mode"] == "continuous":
            raise ContractError("SQL Server connector V1 does not support continuous read")
        columns, _, unique_keys = self._metadata(contract)
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
            if not any(set(key) <= {f.lower() for f in pfields} for key in unique_keys):
                raise ContractError("ordered_fields must include a source unique key for deterministic progress")
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
        columns, pk, _ = self._metadata(contract)
        db, _, _ = self._resource(contract)
        resource = contract.document["source"]["resource"]
        conn = self._connect()
        try:
            cur = conn.cursor()
            qualified = f"{db}.{quoted(resource['schema'])}.{quoted(resource['table'])}"
            cur.execute(f"USE {db}")
            table_name = f"{quoted(resource['schema'])}.{quoted(resource['table'])}"
            cur.execute("SELECT OBJECT_ID(?)", (table_name,))
            object_id = cur.fetchone()[0]
            if object_id is None:
                raise ContractError("SQL Server source table not found")
            try:
                cur.execute("SELECT COALESCE(SUM(row_count),0) FROM sys.dm_db_partition_stats "
                            "WHERE object_id=? AND index_id IN (0,1)", (object_id,))
            except Exception:  # noqa: BLE001 - metadata DMV access may be unavailable
                logging.getLogger(__name__).warning("partition DMV unavailable; using catalog fallback")
                cur.execute("SELECT COALESCE(SUM([rows]),0) FROM sys.partitions "
                            "WHERE object_id=? AND index_id IN (0,1)", (object_id,))
            estimated_rows = int(cur.fetchone()[0] or 0)
            cur.execute("SELECT i.index_id,i.is_primary_key,i.is_unique,i.type_desc,"
                        "ic.key_ordinal,ic.is_included_column,c.name "
                        "FROM sys.indexes i JOIN sys.index_columns ic ON "
                        "i.object_id=ic.object_id AND i.index_id=ic.index_id "
                        "JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id "
                        "WHERE i.object_id=? AND i.is_hypothetical=0 "
                        "ORDER BY i.index_id,ic.key_ordinal,ic.index_column_id", (object_id,))
            index_rows = cur.fetchall()
            by_index: dict[int, list[tuple]] = {}
            index_props = {}
            for row in index_rows:
                index_id = int(row[0])
                by_index.setdefault(index_id, []).append((str(row[6]).lower(), int(row[4]), bool(row[5])))
                index_props[index_id] = (bool(row[1]), bool(row[2]), str(row[3]))
            indexes = []
            included_indexes = []
            key_info: dict[str, tuple[int, float, bool]] = {}
            keys = [pk] if pk else []
            for index_id, members in by_index.items():
                ordered = tuple(name for name, pos, included in members if pos > 0 and not included)
                if not ordered:
                    continue
                indexes.append(ordered)
                included_indexes.append(tuple(name for name, _, included in members if included))
                primary, unique, kind = index_props[index_id]
                if unique and ordered not in keys:
                    keys.append(ordered)
                for pos, name in enumerate(ordered, 1):
                    quality = (1.0 if pos == 1 else 0.25) + (0.5 if kind == "CLUSTERED" else 0)
                    if primary:
                        quality += 0.5
                    old = key_info.get(name)
                    if old is None or quality > old[1]:
                        key_info[name] = (pos, quality, unique and len(ordered) == 1)
            stats: dict[str, tuple[float, int, str | None, tuple, tuple, int | None, float, float]] = {}
            try:
                cur.execute("SELECT s.stats_id,c.name,p.last_updated,p.rows,p.rows_sampled,"
                            "p.modification_counter FROM sys.stats s "
                            "JOIN sys.stats_columns sc ON s.object_id=sc.object_id AND "
                            "s.stats_id=sc.stats_id AND sc.stats_column_id=1 "
                            "JOIN sys.columns c ON c.object_id=sc.object_id AND c.column_id=sc.column_id "
                            "OUTER APPLY sys.dm_db_stats_properties(s.object_id,s.stats_id) p "
                            "WHERE s.object_id=? AND s.has_filter=0", (object_id,))
                stats_rows = cur.fetchall()
            except Exception:  # noqa: BLE001 - statistics DMV access may be unavailable
                logging.getLogger(__name__).warning("statistics properties unavailable")
                stats_rows = []
            for stat_id, raw_name, updated, rows, sampled, modified in stats_rows:
                name = str(raw_name).lower()
                if name not in key_info or not rows:
                    continue
                sample_fraction = min(1.0, float(sampled or 0) / max(1, int(rows)))
                modification_fraction = min(1.0, float(modified or 0) / max(1, int(rows)))
                confidence = max(0.05, (0.3 + 0.7 * sample_fraction) * (1 - modification_fraction))
                if updated and (datetime.now() - updated).days > 30:
                    confidence *= 0.7
                if name in stats and stats[name][0] >= confidence:
                    continue
                try:
                    typ = sql_type_to_canonical(columns[name]["type"],
                                                columns[name]["precision"], columns[name]["scale"])
                except ContractError:
                    continue
                physical_type = columns[name]["type"].lower()
                if physical_type == "date":
                    boundary_sql = "CONVERT(nvarchar(30),CONVERT(date,range_high_key),23)"
                elif physical_type == "datetimeoffset":
                    boundary_sql = "CONVERT(nvarchar(50),CONVERT(datetimeoffset(6),range_high_key),127)"
                elif physical_type in _TEMPORAL:
                    boundary_sql = "CONVERT(nvarchar(40),CONVERT(datetime2(6),range_high_key),126)"
                else:
                    boundary_sql = "CONVERT(nvarchar(4000),range_high_key)"
                try:
                    cur.execute(f"SELECT {boundary_sql},range_rows,equal_rows,distinct_range_rows "
                                "FROM sys.dm_db_stats_histogram(?,?) ORDER BY step_number",
                                (object_id, stat_id))
                    steps = cur.fetchall()
                except Exception:  # noqa: BLE001 - one histogram may be unavailable
                    logging.getLogger(__name__).warning("histogram unavailable for %s", name)
                    continue
                def parse_key(raw, logical_type=typ):
                    if raw is None:
                        return None
                    if logical_type.startswith("int"):
                        return int(raw)
                    if logical_type.startswith("decimal("):
                        return Decimal(raw)
                    return str(raw)
                histogram = tuple((parse_key(bound), max(0, round(float(range_rows or 0) + float(equal_rows or 0))))
                                  for bound, range_rows, equal_rows, _ in steps if bound is not None)
                distinct = sum(int(distinct_rows or 0) + (1 if float(equal_rows or 0) > 0 else 0)
                               for bound, _, equal_rows, distinct_rows in steps if bound is not None)
                hot = tuple(sorted(((parse_key(bound), round(float(equal_rows or 0)))
                                    for bound, _, equal_rows, _ in steps
                                    if bound is not None and float(equal_rows or 0) > 0),
                                   key=lambda item: (-item[1], str(item[0])))[:32])
                null_rows = sum(float(equal_rows or 0) for bound, _, equal_rows, _ in steps if bound is None)
                skew = distribution_skew(histogram, typ)
                stats[name] = (confidence, int(stat_id), updated.isoformat() if updated else None,
                               histogram, hot, distinct, min(1.0, null_rows / max(1, rows)), skew)
            profiles = []
            for name, col in columns.items():
                try:
                    typ = sql_type_to_canonical(col["type"], col["precision"], col["scale"])
                except ContractError:
                    continue
                minimum = maximum = None
                if name in key_info and (typ.startswith(("int", "decimal(")) or typ in {"date", "timestamp"}):
                    sql_name = quoted(col["name"])
                    cur.execute(f"SELECT TOP 1 {sql_name} FROM {qualified} WHERE {sql_name} IS NOT NULL ORDER BY {sql_name} ASC")
                    row = cur.fetchone()
                    minimum = _position_value(row[0]) if row else None
                    cur.execute(f"SELECT TOP 1 {sql_name} FROM {qualified} WHERE {sql_name} IS NOT NULL ORDER BY {sql_name} DESC")
                    row = cur.fetchone()
                    maximum = _position_value(row[0]) if row else None
                stat = stats.get(name)
                if stat and stat[3]:
                    histogram = stat[3]
                    if minimum is None:
                        minimum, maximum = histogram[0][0], histogram[-1][0]
                position, quality, unique = key_info.get(name, (None, 0.0, False))
                profiles.append(ColumnProfile(name, typ, col["nullable"], minimum, maximum,
                                              indexed=name in key_info, unique=unique,
                                              null_fraction=stat[6] if stat else 0.0,
                                              skew=stat[7] if stat else 1.0,
                                              histogram=stat[3] if stat else (),
                                              hot_values=stat[4] if stat else (),
                                              distinct_estimate=stat[5] if stat else None,
                                              statistics_confidence=stat[0] if stat else 0.0,
                                              statistics_updated_at=stat[2] if stat else None,
                                              key_position=position, index_quality=quality))
            widths = {"boolean": 1, "int8": 1, "int16": 2, "int32": 4, "int64": 8,
                      "float32": 4, "float64": 8, "date": 4, "timestamp": 8,
                      "binary": 64, "string": 64}
            row_bytes = sum(widths.get(typ, 16) for _, typ in contract.fields)
            probe_rows = 0
            probe_bytes = 0
            probe_seconds = None
            if self.probe_rows and estimated_rows:
                try:
                    conn.timeout = self.probe_timeout_seconds
                    projection = ", ".join(quoted(name) for name, _ in contract.fields)
                    started = time.monotonic()
                    cur.execute(f"SELECT TOP ({self.probe_rows}) {projection} FROM {qualified}")
                    for batch in cur.arrow_reader(batch_size=min(256, self.probe_rows)):
                        probe_rows += batch.num_rows
                        probe_bytes += batch.nbytes
                        if probe_bytes >= self.probe_max_bytes:
                            break
                    probe_seconds = max(0.000001, time.monotonic() - started)
                except Exception:  # noqa: BLE001 - optional bounded calibration
                    logging.getLogger(__name__).warning("Arrow calibration probe unavailable")
                    probe_rows = probe_bytes = 0
                    probe_seconds = None
                finally:
                    conn.timeout = self.query_timeout_seconds
            schema_hash = fingerprint([(name, columns[name]["type"], columns[name]["precision"],
                                        columns[name]["scale"], columns[name]["nullable"])
                                       for name in sorted(columns)])
            cur.execute("SELECT snapshot_isolation_state, is_read_committed_snapshot_on "
                        "FROM sys.databases WHERE name=DB_NAME()")
            setting = cur.fetchone()
            consistency = tuple(name for enabled, name in
                                ((setting[0] == 1, "SNAPSHOT_ISOLATION_AVAILABLE"),
                                 (bool(setting[1]), "READ_COMMITTED_SNAPSHOT_ON")) if enabled)
            return SourceProfile(uuid.uuid4().hex, estimated_rows, max(1, row_bytes), schema_hash,
                                 tuple(profiles), tuple(keys), 0.6,
                                 tuple(columns), probe_bytes / probe_rows if probe_rows else None,
                                 probe_rows, probe_seconds,
                                 probe_bytes / probe_seconds if probe_rows and probe_seconds else None,
                                 tuple(indexes), tuple(included_indexes), consistency)
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
        elif partition.kind in {"VALUE", "VALUE_RANGE"}:
            clauses.append(f"{quoted(partition.field)} = ?")
            params.append(partition.value)
            if partition.kind == "VALUE_RANGE":
                field = quoted(partition.secondary_field)
                if partition.secondary_lower is not None:
                    clauses.append(f"{field} >= ?")
                    params.append(partition.secondary_lower)
                if partition.secondary_upper is not None:
                    clauses.append(f"{field} < ?")
                    params.append(partition.secondary_upper)
        elif partition.kind == "OTHER":
            field = quoted(partition.field)
            clauses.append(f"{field} IS NOT NULL")
            if partition.value:
                clauses.append(f"{field} NOT IN (" + ",".join("?" for _ in partition.value) + ")")
                params.extend(partition.value)
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
