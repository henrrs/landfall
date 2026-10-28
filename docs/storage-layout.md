# Storage layout

Platform Config maps each domain to one storage account and a configurable `landing` container. Within that container the dataset prefix is `<sourceSystem>/<asset>`; source technology and physical table remain metadata, not path identity.

```text
<account>/<landing-container>/<sourceSystem>/<asset>/
  data/run=<id>/part-00000.arrow
  _ingestion/dataset.json
  _ingestion/profiles/<id>.json
  _ingestion/plans/<id>.json
  _ingestion/runs/<id>/run.json
  _ingestion/runs/<id>/manifest.json
  _ingestion/runs/<id>/partitions/<id>.json
  _ingestion/state/current.json
```

Data files and run evidence are immutable by run ID. `current.json` is small and mutable with compare-and-swap. Do not apply temporary-data retention rules to state, manifests, plans or profiles; removing them breaks recovery and audit.
