# SQL Server connector

The SQL Server adapter resolves a credential through `connectionRef`, connects with `mssql-python`, and validates `database/schema/table`. All identifiers are restricted to safe SQL identifier syntax and quoted. Values are bound as parameters. Metadata comes from `INFORMATION_SCHEMA`; primary key columns are required to make `ordered_fields` deterministic. The source profile uses `sys.partitions` row estimates and bounded primary-key min/max queries. It avoids a full-table `COUNT(*)`.

The extraction query projects only contracted fields. Filters compile from the contract AST to parameterized SQL; incremental bounds use lexicographic predicates; physical ranges add another conjunct. `cursor.arrow_reader()` streams RecordBatches directly. The adapter closes each connection when its partition ends.

`tinyint` maps to `int16` because SQL Server tinyint is unsigned. SQL `timestamp`/`rowversion` maps to `binary`, never a temporal type. `time` has no V1 canonical scalar and fails explicitly. Unsupported or mismatched source types fail before capture. SQL Server source-native CDC is outside V1.
