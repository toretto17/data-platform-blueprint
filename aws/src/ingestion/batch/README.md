# batch

Pull a batch source (S3 files OR JDBC database) into the Bronze landing  zone. Thin wrapper that feeds the Bronze job; keeps source-connection  concerns (JDBC, partitioned reads, secrets) in one place

## Files

- `batch_ingest.py`

## Platform twin

`./databricks/src/ingestion/batch/`
