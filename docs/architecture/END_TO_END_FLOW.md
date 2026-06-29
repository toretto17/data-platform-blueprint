# End-to-End Data Platform Flow

```
┌─────────────────── DATA ENGINEERING ───────────────────────────────────────┐
│                                                                             │
│  Sources (JDBC/S3/Kafka)                                                    │
│      ↓                                                                      │
│  [INGESTION] batch/stream → [BRONZE] raw+lineage (append-only)              │
│      ↓                                                                      │
│  [SILVER] cleanse, dedup, cast, DQ                                          │
│      ↓                                                                      │
│  [GOLD] aggregations, windows (MTD/YTD), zero-fill, round                   │
│      ↓                                                                      │
│  [CONSUMPTION] → Redshift / Databricks SQL / Snowflake / REST API           │
│                                                                             │
├─────────────────── ML / DATA SCIENCE ─────────────────────────────────────┤
│                                                                             │
│  Gold/Silver → [FEATURE ENGINEERING] lags, rolling, calendar                │
│      ↓                                                                      │
│  [FEATURE STORE] UC table / SageMaker FeatureGroup (PIT joins)              │
│      ↓                                                                      │
│  [TRAINING] FS → train → evaluate (gate) → register (UC / Model Registry)  │
│      ↓                                                                      │
│  [INFERENCE] batch (score_batch / Batch Transform) + realtime (endpoint)    │
│      ↓                                                                      │
│  [CONSUMPTION] predictions → Gold → BI / downstream                         │
│                                                                             │
├─────────────────── OPERATIONS ────────────────────────────────────────────┤
│                                                                             │
│  Orchestration: Step Functions / Databricks Workflows / EventBridge / cron   │
│  Monitoring:    DQ alerts, drift detection (PSI/KS), SLA tracking            │
│  CI/CD:         PR → lint+test → deploy scripts → Terraform/DAB apply       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Load patterns used
- **Incremental**: Silver (watermark-based, append/upsert)
- **Full**: Gold (monthly overwrite with dynamic partitions)
- **CDC**: from source systems (DMS/Delta CDF → MERGE)
- **SCD2**: dimensions with history (two-step MERGE)
