# Consumption — AWS

Final reporting/serving layer. Gold → Consumption → BI/API.

| Subfolder | File | Purpose |
|---|---|---|
| `jobs/` | `consumption_job.py` | Build the consumption table (INITIAL_LOAD vs daily) — Athena/Glue |
| `warehouse/` | `warehouse_load.py` | Load to **Amazon Redshift** via the Spectrum→native pattern (DELETE+INSERT, transactional, idempotent) |
| `snowflake/` | `snowflake_load.py` | Write to **Snowflake** (Spark connector; overwrite/append/merge; creds via Secrets Manager) |
| `apis/` | `api_serving.py` | REST API over consumption (Athena) — **API-key auth enabled**, table allow-list |

## Best practices
- Redshift load is **idempotent** (DELETE the window then INSERT) and **transactional** (BEGIN/COMMIT) so BI never sees partial data.
- Spectrum reads the Glue Catalog directly — no brittle mirror job.
- API ships with auth ON + table allow-list (prevents arbitrary access/injection). Prefer API Gateway + Cognito/JWT in prod.
- Secrets always from Secrets Manager, never hardcoded.
