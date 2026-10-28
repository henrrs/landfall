# Local lab

`lab/compose.yaml` starts SQL Server 2022, Floci AZ (ADLS Gen2), the Azure Key Vault emulator, and a wheel-only runner. `make e2e` builds `dist/landfall-0.1.0-py3-none-any.whl`, builds the runner with `pip install` of that wheel, seeds the CRM table and Key Vault secret, runs two incremental captures and verifies projected Arrow rows and durable state. Docker Compose is required.

```bash
make e2e
```

For inspection, use `make lab-up`, `make lab-seed`, `make lab-run`, `make lab-test` and `make lab-down`. `make lab-reset` deletes the lab volumes. `make lab-env` generates a random local storage key in ignored `lab/.env`; the generated Key Vault TLS certificate is under ignored `lab/certs/`. The SQL password is a lab-only default that can be overridden with `LANDFALL_LAB_SQL_PASSWORD`.

`lab/platform-config/e2e.yaml` selects ADLS and Key Vault adapters. Its `dfsHostHeader` setting is only for Floci's path-style routing. Its tiny partition size forces a physical SQL range plan, so the lab checks that partition predicates preserve the incremental semantic scope. The contract under `contracts/examples/` follows the [normative V1 grammar](../ingestion-contract/contrato_ingestao_v1.md) and contains neither service endpoint nor secret. The E2E runner imports the installed wheel and does not mount the checkout's `src/` directory.
