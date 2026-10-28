# Landfall

Landfall is a source-to-landing capture engine. It reads an independent **Ingestion Contract V1** plus a separate **Platform Config**, and produces immutable Arrow IPC files, a committed run manifest, and a durable capture checkpoint. SQL Server is the supported source, using `mssql-python`.

```text
Ingestion Contract + State → SemanticReadScope → SourceProfile + History
        → PhysicalReadPlan → Local or Spark runtime → Arrow IPC → Landing → Manifest → State
```

The [normative Ingestion Contract V1](ingestion-contract/contrato_ingestao_v1.md) defines source selection, authorized fields, filters, read semantics and target intent. Platform Config resolves secrets, storage, compression and planner tuning. `target.write` is validated and recorded; it never deletes or deduplicates Landing captures.

## Run

```bash
make setup
export LANDFALL_CONNECTION_CRM_LOCAL='{"server":"localhost","port":11433,"database":"CRM","user":"sa","password":"...","trustServerCertificate":"yes"}'
landfall validate-contract --contract contracts/examples/customer.incremental.yaml
landfall plan --contract contracts/examples/customer.incremental.yaml --platform lab/platform-config/local.yaml
landfall run --contract contracts/examples/customer.incremental.yaml --platform lab/platform-config/local.yaml
```

`LANDFALL_CONTRACT_JSON` or `LANDFALL_CONTRACT_PATH` can supply the same contract to the CLI. The connection reference is resolved outside the contract.

## Build and test

```bash
make test-all-nospark
make test-all-spark
make test-all
```

Certification commands require Docker and Docker Compose. Spark certification uses `mapInArrow` with a single-node Spark session and a two-worker Standalone cluster. See [Spark runtime](docs/spark-runtime.md), [testing strategy](docs/testing-strategy.md), and [local lab](docs/local-lab.md). Runtime boundaries and limitations are tracked in [V1 completion](docs/v1-completion.md).

Detailed design and boundaries: [architecture](docs/architecture.md), [contract integration](docs/ingestion-contract-integration.md), [source position](docs/source-position.md), [planner](docs/adaptive-planner.md), [storage](docs/storage-layout.md), [tests](docs/testing-strategy.md).
