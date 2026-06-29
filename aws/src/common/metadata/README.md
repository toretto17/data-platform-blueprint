# 📊 common/metadata — aws — ☁️ AWS


| File | Purpose |
|---|---|
| `audit.py` | One audit row per job run (run_id, rows in/out, status, duration, error). Sink: DynamoDB or S3. |
| `freshness.py` | Skip reprocessing when nothing new. Max partition + S3 marker watermark. |

Use `audit` for observability/compliance; `freshness` to avoid redundant runs.
Same API names across platforms.


---

> 🔄 **Platform twin:** `./databricks/src/common/metadata/`
