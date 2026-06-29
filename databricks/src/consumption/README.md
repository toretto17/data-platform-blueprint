# 🍽️ Consumption — Databricks — 🧱 Databricks


Final reporting/serving layer. Gold → Consumption → Databricks SQL / API.

| Subfolder | File | Purpose |
|---|---|---|
| `jobs/` | `consumption_job.py` | Build the consumption Delta table (INITIAL_LOAD vs daily) + UC view |
| `warehouse/` | `warehouse_load.py` | Build **Databricks SQL serving** (serving Delta table or Materialized View) + governed UC view |
| `snowflake/` | `snowflake_load.py` | Write to **Snowflake** (Spark connector; creds via secret scope) |
| `apis/` | `api_serving.py` | REST API over consumption (Databricks SQL Warehouse) — **API-key auth enabled** |

## Best practices
- Lakehouse has no separate warehouse to copy into — Databricks SQL queries Delta/UC directly; "warehouse load" = build the governed serving object.
- Materialized Views auto-refresh; serving tables use `replaceWhere` for idempotent partition refresh.
- API auth ON + allow-list. Prefer OAuth/JWT/SSO in prod. Token from secret scope.


---

> 🔄 **Platform twin:** `./aws/src/consumption/`
