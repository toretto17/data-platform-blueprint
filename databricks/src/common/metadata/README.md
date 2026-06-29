# common/metadata — databricks

| File | Purpose |
|---|---|
| `audit.py` | One audit row per job run (run_id, rows in/out, status, duration, error). Sink: Delta audit table. |
| `freshness.py` | Skip reprocessing when nothing new. Max partition, Delta DESCRIBE HISTORY, watermark table. |

Use `audit` for observability/compliance; `freshness` to avoid redundant runs.
Same API names across platforms.
