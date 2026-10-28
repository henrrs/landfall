# Ingestion Contract integration

The [official V1 specification](../ingestion-contract/contrato_ingestao_v1.md) is normative and takes precedence over this integration guide. Landfall consumes `apiVersion: ingestion.contract/v1alpha1` and `kind: IngestionContract` without renaming them. `fields` is the extraction allowlist; SQL queries project exactly those columns. `read.filters`, `read.initialPosition` and `read.position` define progressive read semantics. The language permits `full`, `incremental` and `continuous`; the SQL Server V1 connector supports the first two and rejects `continuous` before reading. `metadata.domain` resolves a storage account in Platform Config, while `source.connectionRef` resolves a secret through a provider.

`target.asset` participates in identity `domain.sourceSystem.asset`. `target.write` and fingerprint deduplication are validated and recorded in runs/manifests. They govern future materialization, not the immutable Landing write. A contract update is prospective; it does not trigger backfill.

The contract never holds current checkpoint, run ID, storage account, container, compression, partition count or runtime selection. The checkpoint belongs to Landing state. Platform Config holds infrastructure, domain/source-system registries and planner tuning independently. YAML and `LANDFALL_CONTRACT_JSON` are two transports for the same validator and model. For `in`/`not_in`, the normative filter key is `values`; scalar operators use `value`.

SQL Server specific checks happen after core validation: resource identifiers, source columns and canonical mappings, filter columns, non-null ordered position fields and primary-key tie breaker. SQL Server V1 rejects `source_native`; the core model reserves it for future connectors.
