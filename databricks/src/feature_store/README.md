# Feature Store

Manage ML features: create, compute, write, validate, and serve for training/inference.

## Files

| Subfolder | File | Purpose |
|---|---|---|
| `creation/` | `feature_group.py` | Create + describe + read the feature table/group |
| `ingestion/` | `feature_store_job.py` | Scheduled compute + write (batch or streaming) |
| `validation/` | `feature_store_validation.py` | DQ checks before training (PK unique, null rate, freshness) |

## Platform-specific notes

### AWS (SageMaker Feature Store)
- A Feature Group = the storage entity (Iceberg offline store + optional online store).
- Ingest via `FeatureStoreManager.ingest_data()` (Spark connector) — large batch writes.
- Read offline via Athena SQL (PIT dedup with ROW_NUMBER over event_time).
- Training set extraction: Athena query → S3 → Spark `read.parquet`.

### Databricks (Feature Engineering in Unity Catalog)
- ANY Delta table with a PK constraint = a feature table (no special entity needed).
- `FeatureEngineeringClient.create_table()` / `write_table()` / `read_table()`.
- Training set: `fe.create_training_set(df, feature_lookups=[FeatureLookup(...)])`.
- Auto lineage tracking + model packaging via `fe.log_model()` + `fe.score_batch()`.
- PIT joins via `timeseries_columns` on the table + `timestamp_lookup_key` on FeatureLookup.
- No Photon dependency — works on any DBR 13.2+. `optimizeWrite` recommended.

## Integration flow

```
Gold/Silver tables → feature_store_job (compute + write) → Feature table
                                                             ↓
         validate (DQ) → training_set (PIT join) → log_model (lineage)
                                                             ↓
         score_batch / serve online (auto-lookup from the table)
```

## Cost-effective defaults
- Freshness guard: skip computation if upstream hasn't changed (free early-exit).
- AQE enabled (auto skew/partition handling — no manual tuning needed).
- `optimizeWrite=true` (fewer S3 objects, less listing overhead).
- Batch `mode='merge'` (upsert — only rows that changed get rewritten).
- No dependency on Photon or online stores unless explicitly opted in.
