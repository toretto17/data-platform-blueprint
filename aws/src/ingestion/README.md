# 📥 Ingestion — ☁️ AWS


Bring external data into the **Bronze** landing zone. Two modes:

| Subfolder | Pattern | Files |
|---|---|---|
| `batch/` | One-shot / scheduled pulls (S3 files, JDBC) | `batch_ingest.py` |
| `streaming/` | Continuous (Kinesis/Kafka) or incremental files (Autoloader) | `stream_ingest.py` |

## AWS
- `batch_ingest.py` — S3 or JDBC (creds from Secrets Manager) → Bronze Parquet.
- `stream_ingest.py` — Kinesis or Kafka via Spark Structured Streaming + **checkpoint** (exactly-once). Run as a Glue Streaming job.

## Databricks
- `batch_ingest.py` — files or JDBC (creds from secret scope) → Bronze Delta.
- `stream_ingest.py` — **Autoloader** (`cloudFiles`, recommended for files) or Kafka → Bronze Delta + **checkpoint**.

## Best practices baked in
- Checkpointing on every stream (fault tolerance + exactly-once).
- `_ingest_ts` / `_ingest_date` lineage stamped on landing.
- Append-only to Bronze, partitioned by `_ingest_date`.
- Secrets never hardcoded (Secrets Manager / secret scopes).
- Autoloader preferred over plain file streaming for schema evolution + scalability.



---

> 🔄 **Platform twin:** `./databricks/src/ingestion/`
