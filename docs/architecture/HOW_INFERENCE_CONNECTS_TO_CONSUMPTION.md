# 🔗 How Inference connects to Consumption

```
Inference output (predictions) → S3/Delta
    ↓
Consumption ETL reads inference output + Gold features
    ↓ enriches (dim_time, geo, RLS columns)
    ↓ merges with existing consumption table
Consumption table (Athena / DBSQL / Redshift)
    ↓
BI dashboards / downstream APIs
```

## Key patterns
- INITIAL_LOAD: writes baseline (Gold history WITHOUT scores)
- Daily: reads new inference parquets, merges WITH scores into consumption
- Metadata file tracks "which run_dates have been processed"
- MAX_BATCH cap prevents OOM on large backlogs (process N per run)
