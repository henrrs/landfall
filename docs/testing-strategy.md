# Testing strategy

`make test` runs architecture, contract conformance, SQL predicate/type conformance, planner, property and local capture tests. Contract conformance parses all nine reference contracts shipped with the normative V1 specification. Architecture tests parse imports and forbid SQL/cloud/runtime dependencies in `core`. Hypothesis exercises numeric range coverage. Local capture tests write and read actual Arrow IPC, check allowlist, incremental advancement, recovery and state compare-and-swap.

`make verify-wheel` installs the built wheel in a fresh venv and exercises the installed CLI. `make e2e` installs the same wheel in a Docker runner, then uses real SQL Server, Key Vault emulator and Floci ADLS Gen2. The runner compares Arrow IDs and state across two runs.

The lab fixture is intentionally small. Broader SQL distribution fixtures, fault injection, real Azure compatibility and performance benchmarks remain extension work. Real Azure tests should use a separate opt-in CI profile, never be required for every pull request.
