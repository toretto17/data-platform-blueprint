# Bronze Layer

The **Bronze layer** is the raw landing zone. Data arrives from source systems and is stored **as-is** — no business logic, no cleansing, no deduplication. We add only ingestion lineage columns and append by ingest date so we keep the full raw history. All cleansing/typing/dedup happens later in **Silver**.

> Mental model: Bronze = "what we received, untouched, with a timestamp". Silver = "cleaned & conformed". Gold = "business-ready aggregates".

---

## Files

| File | Platform | What it does |
|---|---|---|
| `jobs/bronze_job_template.py` | AWS Glue | Raw (S3/Catalog/JDBC) → Bronze Parquet + Glue Catalog, append |
| `jobs/bronze_job_databricks.py` | Databricks | Raw (Autoloader/batch) → Bronze Delta (UC), append |
| `transformations/` | both | (optional) raw-shape helpers — usually empty for Bronze |
| `dq/` | both | (optional) light raw DQ checks |

Both job files mirror the structure of `src/silver/jobs/silver_job_template.py` and `src/gold/marts/gold_job_template.py` — same `Base*Job` class shape, `_configure_spark`, override methods, `run()`, and an example subclass at the bottom.

---

## What to change (fill-in guide)

### AWS (`bronze_job_template.py`)
1. `_define_sources()` — point to your raw source:
   - S3: `{"type":"s3","path":"s3://.../raw/","format":"parquet"}`
   - Catalog: `{"type":"catalog","db":"src_db","table":"raw_tbl"}`
   - JDBC: `{"type":"jdbc","url":"jdbc:...","table":"schema.tbl","secret":"sm-id"}`
2. `_add_audit_columns()` — add any extra lineage you want (default stamps `_ingest_ts`, `_ingest_date`, `_source_file`, `_source_system`).
3. `_get_dq_ruleset_name()` — return a Glue DQ ruleset name, or `None` to skip.
4. Set args in DDB config / Glue DefaultArguments (see below).

### Databricks (`bronze_job_databricks.py`)
1. `_define_source()` — `{"path": "...", "format": "parquet"}`.
2. Choose `use_autoloader=true` (incremental, recommended) or `false` (batch).
3. Set `catalog`, `schema`, `table`, `checkpoint_path` job parameters.
4. For full DQ, use Delta Live Tables expectations (see `src/common/validations`).

---

## Best practices for Bronze (enforced in the templates)

- **Append-only.** Never overwrite Bronze — it is the source of truth for raw history. (`MODE=append`.)
- **No business logic.** Don't filter, dedup, or cast business columns here. Keep it raw.
- **Always add lineage.** `_ingest_ts`, `_ingest_date`, `_source_file`, `_source_system`.
- **Partition by `_ingest_date`.** Cheap to prune, aligns with how data lands.
- **Schema evolution on.** Sources drift — Bronze should absorb new columns (Glue: crawler/`MSCK REPAIR`; Databricks: `mergeSchema`/Autoloader schema location).
- **Fail fast on missing args.** No silent nonprod fallback.
- **Idempotent re-runs.** Autoloader checkpoints (Databricks) or dedup by `_source_file` downstream (AWS).
- **Early exit** when there's no new data (no wasted compute).

---

## Run

### AWS (Glue)
Triggered by the framework Step Function via DDB config, or directly:
```bash
aws glue start-job-run \
  --job-name glue_<project>_<feature>_bronze_<entity> \
  --arguments '{
    "--SOURCE_TYPE":"s3",
    "--SOURCE_PATH":"s3://CHANGE_ME/raw/sales/",
    "--TARGET_BUCKET":"s3-...-bronze-.../sales_analytics",
    "--TARGET_DATABASE":"sales_analytics_bronze",
    "--TARGET_TABLE":"bronze_sales",
    "--PARTITION_COLUMN":"_ingest_date",
    "--MODE":"append",
    "--source_system":"sales_raw",
    "--data_date":"2026-06-28"
  }' --region ap-southeast-1
```

### Databricks
As a Databricks Job with parameters, or run the notebook:
```python
cfg = {
  "catalog": "main", "schema": "bronze", "table": "bronze_sales",
  "source_path": "s3://CHANGE_ME/raw/sales/", "source_format": "parquet",
  "checkpoint_path": "s3://CHANGE_ME/_checkpoints/bronze_sales",
  "use_autoloader": "true", "mode": "append", "source_system": "sales_raw",
}
BronzeSalesJobDatabricks(cfg).run()
```

SQL-only equivalent is documented at the top of `bronze_job_databricks.py`.

---

## Relationship to other layers

```
Source systems → [BRONZE: raw + lineage, append]
                      → [SILVER: cleanse, dedup, cast]
                          → [GOLD: business aggregates]
                              → [CONSUMPTION: reporting / serving]
```
