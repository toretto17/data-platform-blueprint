# ⚡ jobs — ☁️ AWS


Template for building Bronze-layer ETL jobs (Raw source → Bronze).  Bronze = the raw landing zone. We keep data AS-IS (no business logic),  add ingestion lineage columns, and append by ingest date so 

## Files

- `bronze_job.py`

## Platform twin

`./databricks/src/bronze/jobs/`
