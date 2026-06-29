# Data Engineering Load Patterns (`de_patterns/`)

Reusable load-pattern templates. Each has **real working code** with both
**PySpark API** and **SQL** variants (SQL creates a temp view first, then runs
the statement). Pick the pattern that matches your source + history needs.

## Files

| File | Pattern | When to use |
|---|---|---|
| `full_load.py` | Truncate & reload / snapshot overwrite | Small source, no watermark/CDC, or guaranteed clean rebuild |
| `incremental_load.py` | Watermark-based append/upsert | Source has a monotonically increasing column (updated_at/id) |
| `cdc_load.py` | Change Data Capture (Delta CDF / DMS) | Source emits row-level inserts/updates/deletes |
| `scd_type1.py` | SCD Type 1 (overwrite, no history) | Keep only current value of a dimension |
| `scd_type2.py` | SCD Type 2 (history + effective dates) | Track every version of a dimension over time |

## Decision matrix

```
Need row-level deletes / true CDC?           → cdc_load
Need dimension history (old + new versions)? → scd_type2
Only current dimension value, overwrite?     → scd_type1
Have a reliable watermark column?            → incremental_load
None of the above / small table?            → full_load
```

## Platform notes

- **AWS** (`aws/src/de_patterns/`): Glue 5.x + Delta on S3. Set job params:
  `--datalake-formats delta` plus the Delta Spark extensions (see each file header).
  CDC supports **DMS change files** (Job Bookmarks) and **Delta CDF**.
- **Databricks** (`databricks/src/de_patterns/`): DBR 15.x LTS + Delta + Unity Catalog.
  CDC uses **Delta Change Data Feed** (`readChangeFeed` / `table_changes()`), batch or
  streaming with checkpointing.

## Key implementation notes

- **CDC**: changes are collapsed to the *net latest change per key* before MERGE
  (drop `update_preimage`, keep highest `_commit_version`). Streaming mode tracks
  the processed version via the **checkpoint** automatically; batch mode persists a
  version/watermark you manage.
- **SCD2**: uses the canonical **two-step MERGE** (mergeKey trick) — one MERGE both
  closes the current row (`is_current=false`, `effective_end=today`) and inserts the
  new version (`is_current=true`). NULL-safe (`<=>`) change detection on tracked cols.
- **Idempotency**: all patterns are safe to re-run (MERGE/overwrite, watermark guard,
  CDF checkpoint). Full load skips if the source is empty (never wipes the target).

## SQL vs PySpark

Every apply method exists in both forms. The SQL variants always show the
`CREATE OR REPLACE TEMP VIEW ...` step first so you can copy them straight into a
Databricks SQL cell / Glue Spark SQL and inspect the staged view before MERGE.

