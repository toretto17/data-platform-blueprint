# 🥈 Silver — databricks — 🧱 Databricks


Cleansed, conformed, deduplicated data (Bronze → Silver).

| File | Purpose |
|---|---|
| `jobs/silver_job.py` | Base Silver job — read source, transform, derive, DQ, write |
| `dq/silver_dq.py` | Silver DQ config (PK not-null, freshness) on the shared DQ framework |

Override `_define_sources`, `_apply_transformations`, `_derive_columns`, `_dq_config`.
Cleansing/dedup/casting happens HERE (not in Bronze). Delta + Unity Catalog.


---

> 🔄 **Platform twin:** `./aws/src/silver/`
