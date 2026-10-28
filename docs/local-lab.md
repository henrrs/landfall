# Local lab

`lab/compose.yaml` starts SQL Server 2022, Floci AZ (ADLS Gen2), the Azure Key Vault emulator, and a wheel-only runner. `make e2e` builds the wheel, installs it in the runner, seeds the CRM table and Key Vault secret, materializes SQL statistics, verifies index/histogram/Arrow calibration profiling, then runs two incremental captures and checks projected Arrow rows, manifest consistency level and durable state. Before the second run it adds an uncontracted source column and verifies that schema drift is observed without changing the projected allowlist. Docker Compose is required.

```bash
make e2e
```

For inspection, use `make lab-up`, `make lab-seed`, `make lab-run`, `make lab-test` and `make lab-down`. `make lab-reset` deletes the lab volumes. `make lab-env` generates a random local storage key in ignored `lab/.env`; the generated Key Vault TLS certificate is under ignored `lab/certs/`. The SQL password is a lab-only default that can be overridden with `LANDFALL_LAB_SQL_PASSWORD`.

`lab/platform-config/e2e.yaml` selects ADLS and Key Vault adapters. Its `dfsHostHeader` setting is only for Floci's path-style routing. Its tiny partition size forces a physical SQL range plan, so the lab checks that partition predicates preserve the incremental semantic scope. The contract under `contracts/examples/` follows the [normative V1 grammar](../ingestion-contract/contrato_ingestao_v1.md) and contains neither service endpoint nor secret. The E2E runner imports the installed wheel and does not mount the checkout's `src/` directory.

The `dbo.Shape` fixture adds negative and sparse IDs, decimal and temporal values, NULLs, an indexed hot `region`, an included index column and a wide string column. The runner hashes canonical rows from a reference SINGLE read and compares them with numeric, decimal, temporal and hierarchical value partitions. The fixture is deliberately small; it proves predicate equivalence, not large-table performance.

`CompositeShape`, `GuidShape`, `EmptyShape` and `NoKeyShape` verify composite key metadata, safe GUID/no-index fallback and empty-source behavior. The composite adaptive read is compared with its reference SINGLE result.

`SkewNumeric` and `SkewTemporal` each contain 199 nearby values and one distant value. They verify that real SQL histograms trigger numeric/temporal quantile planning and that the resulting partitions match the reference reader with lower maximum row count than equal-width ranges.

`HotShape` verifies automatic hierarchical splitting of an indexed hot value. `WideShape` exercises MAX strings/binary, XML and rowversion with observed Arrow width; `UniqueNoPk` verifies ordered progress using a unique index without a primary key.

`CompositeHot` verifies the numeric composite-key strategy against a reference SINGLE read, including a hot leading key and a secondary range.
