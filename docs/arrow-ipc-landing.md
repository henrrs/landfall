# Arrow IPC Landing

Each partition streams Arrow RecordBatches into an Arrow **IPC File** (`.arrow`). Landfall casts batches to the contracted canonical schema, closes the file and reads it back before publication. Compression is Platform Config `none`, `lz4` or `zstd` (default `lz4`).

Local storage copies to `.partial`, fsyncs and atomically renames. The ADLS adapter uploads a uniquely named partial file then renames it through the hierarchical namespace. A partition success record is written only after publication. The run manifest is the commit index for future consumers; listing data files is insufficient to determine a valid run.

The current ADLS adapter validates the local IPC file before upload and checks that the published path exists. Large orphaned published files recovered after a crash are read into memory for validation; this path should be replaced with a streaming download for very large files.
