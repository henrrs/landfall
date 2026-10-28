# Testing strategy

`make test` runs architecture, normative contract and SQL conformance, planner, property and local capture tests. The normative suite loads all nine independent examples and rejects drift in required fields, read/write semantics, physical settings and fingerprint rules. Property tests cover numeric, decimal, temporal, quantile and hierarchical partition coverage. Local integration checks Arrow files, checkpoint advancement, lease exclusion, history feedback, orphan recovery without whole-object RAM loading and resume after a state write failure.

`make verify-wheel` installs the built wheel in a fresh venv and exercises the installed CLI. A separate `lab-e2e` CI job runs on main pushes or manual dispatch. `make e2e` installs the wheel in a Docker runner with real SQL Server, Key Vault emulator and Floci ADLS Gen2. The runner verifies SQL profiling, compares reference SINGLE with partitioned SQL readers across multiple shapes, measures row balance on skewed numeric/temporal fixtures, and compares Arrow IDs and state across two incremental runs.

The lab fixture remains small. Broader SQL distribution fixtures, shared-snapshot experiments, real Azure compatibility and performance benchmarks remain incomplete. Real Azure tests should use a separate opt-in CI profile.
