# ⚠️ Platform-Specific Gotchas

## AWS Glue
- Glue 5.x uses Spark 3.5 — check function compatibility (e.g. `try_cast` not available)
- `pushDownPredicate` only works on Hive-partitioned tables (not Delta)
- Job Bookmarks track files by modification time — re-uploading a file re-processes it
- `--datalake-formats delta` MUST be set for Delta reads/writes
- Dynamic partition overwrite: set `partitionOverwriteMode=dynamic` explicitly

## Databricks
- Photon is NOT always enabled — don't assume `photonEnabled=true`
- `dbutils.secrets.get` is only available in notebooks/jobs — use the `DBUtils(spark)` fallback in .py files
- Unity Catalog: `CREATE TABLE` on managed tables auto-assigns location — don't specify path
- Delta CDF (legacy) needs `enableChangeDataFeed=true` on the table; automatic CDF needs DBR 18+
- Autoloader `schemaLocation` must be a persistent path (not ephemeral) — use S3/DBFS

## Both
- Never use `.count()` for emptiness check (full scan). Use `len(df.head(1)) == 0`.
- COALESCE(col, 0) doesn't work if col=0 (not NULL). Use NULLIF(col, 0) first.
- Window PARTITION BY must include ALL dimensions — else values bleed across groups.
- SCD2 MERGE needs the "two-step mergeKey trick" — one MERGE can't both UPDATE and INSERT for the same source key.
- Round all float columns before writing (banker's rounding differences between Spark and Python).
