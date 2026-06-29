# 📦 How to: Set Up Batch Ingestion

## From S3 files

### Databricks
```python
cfg = {"source_type": "files", "source_path": "s3://raw-bucket/sales/",
       "source_format": "parquet", "target_table": "main.bronze.sales"}
BatchIngestDatabricks(cfg).run()
```

### AWS Glue
```bash
aws glue start-job-run --job-name my-ingest-job \
  --arguments '{"--SOURCE_TYPE":"s3","--SOURCE_PATH":"s3://raw/sales/","--TARGET_TABLE":"bronze_db.sales"}'
```

## From a database (JDBC)

### Databricks
```python
cfg = {"source_type": "jdbc", "secret_scope": "prod-db", "secret_user_key": "user",
       "secret_pwd_key": "password", "jdbc_url": "jdbc:postgresql://host:5432/db",
       "jdbc_table": "public.customers", "target_table": "main.bronze.customers"}
BatchIngestDatabricks(cfg).run()
```

### AWS Glue
```bash
aws glue start-job-run --job-name my-jdbc-ingest \
  --arguments '{"--SOURCE_TYPE":"jdbc","--JDBC_URL":"jdbc:postgresql://host:5432/db",
    "--JDBC_TABLE":"public.customers","--JDBC_SECRET":"prod/db/credentials",
    "--TARGET_TABLE":"bronze_db.customers"}'
```
Note: JDBC driver must be on `--extra-jars`.

## What the template adds automatically
- `_ingest_ts` (timestamp of ingestion)
- `_ingest_date` (partition column, YYYYMMDD)
- `_source_file` (which file the row came from)
- `_source_system` (label for lineage)
