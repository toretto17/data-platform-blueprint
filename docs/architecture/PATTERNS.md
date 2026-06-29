# 🏛️ Architecture Patterns — Extracted from Production

## 1. Data Engineering Patterns

### Medallion Architecture (Bronze → Silver → Gold → Consumption)
- **Bronze/Raw**: Cross-account federated Glue Catalog (read-only)
- **Silver**: Cleansed, typed, deduplicated, partitioned by `mnth_id`
- **Gold**: Business aggregations, window functions, dimension joins
- **Consumption**: BI-ready, zero-filled, Redshift-loaded

### Configuration-Driven ETL
- All job parameters stored in DynamoDB (not hardcoded)
- Framework Step Function reads DDB → passes args to Glue job
- Same Glue script works across dev/prod via DDB params
- Placeholder rendering: `${environment}`, `${account_id}` at deploy time

### Incremental Processing
- `LOOKBACK_DAYS` parameter controls how far back to reprocess
- `LOOKBACK_DAYS=0` = full reload (initial load)
- Daily runs: `LOOKBACK_DAYS=60` (reprocess last 2 months)
- Dynamic partition overwrite: only touched partitions updated

### DQ Pattern: Warn + Skip
```python
def get_ruleset(self, name):
    try:
        return self.glue_client.get_data_quality_ruleset(Name=name)
    except EntityNotFoundException:
        logger.warning(f"DQ ruleset '{name}' not found — skipping")
        return None
```
DQ must NEVER crash the pipeline. Missing rulesets = warning only.

---

## 2. MLOps Patterns

### Config Tiers
- **Tier A** (Recipe): Code-versioned with image. Change = new build.
- **Tier B** (Tunables): DDB parameters. Change = no rebuild.
- **Tier C** (Infra): Account/bucket/role. Env-overridable via `MLOPS_*` vars.

### Train → Evaluate → Register → Promote
- SageMaker Pipelines for managed DAG
- Model Registry for versioning + approval gates
- Batch Transform for offline scoring
- Feature Store (Iceberg) for point-in-time features

### Feature Store Integration
- Spark connector JAR loaded in Glue
- Freshness guard: skip if FS already has latest data
- Auto-create Feature Groups if missing
- record_id + event_time mandatory columns

---

## 3. Infrastructure Patterns

### Terraform Structure
```
terraform/
├── modules/          # Reusable: glue, sfn, eventbridge, s3
├── workload/         # Stack definitions (etl-pipeline, ml-pipeline)
└── env/              # Per-env tfvars (dev, prod)
```

### Naming Convention (S3 Buckets)
`s3-{project}-{feature}-{env}-{layer}-{account_id}`

### IAM Least Privilege
- Glue role: S3 read/write to specific buckets, Glue Catalog, DDB read
- SageMaker role: Feature Store ops, Model Registry, S3
- Step Functions role: StartExecution on child SFs, InvokeGlue

---

## 4. Orchestration Patterns

### Master Pipeline → Framework SF → Glue Job
```
EventBridge (cron) → Master SF (domain-specific DAG)
    → Framework SF (generic: read DDB → start Glue → Redshift load)
        → Glue Job (actual ETL logic)
```

### Parallel + Sequential
- Silver + Dimension jobs run in PARALLEL
- Consumption waits for BOTH to complete
- Failure in any branch → Fail state (no partial processing)

### EventBridge Scheduling
- Separate schedules per pipeline (staggered to avoid resource contention)
- Disable during initial load / maintenance

---

## 5. Security Patterns

### Cross-Account Data Access
- Federated Glue Catalog (source account grants access via Resource Policy)
- No data copying — read directly from source S3

### Secrets Management
- No secrets in code/config files
- IAM roles for service-to-service auth
- KMS encryption for S3 at rest

### Environment Isolation
- Separate AWS accounts for dev/prod
- Separate IAM roles per environment
- No cross-environment data leakage

---

## 6. Monitoring Patterns

### Job Health
- CloudWatch metrics: duration, status, DPU usage
- SNS alerts on FAILED status
- Slack integration for team notifications

### Data Freshness
- Check latest partition timestamp
- Alert if data not refreshed within SLA

### Cost Tracking
- Tag all resources (Environment, Project, ManagedBy)
- CloudWatch cost metrics per job
- Right-size workers based on actual usage

---

## 7. Common Bug Patterns (from production)

| # | Pattern | Prevention |
|---|---------|-----------|
| 1 | Wrong column for different products | Check column population per product before COALESCE |
| 2 | Missing dimension in window PARTITION BY | ALWAYS include all grain columns |
| 3 | Missing dimension in JOIN condition | Full key in ON clause |
| 4 | No rounding before write | `round_all_floats()` as final step |
| 5 | SUM on group-level columns without dedup | DISTINCT first, then aggregate |
| 7 | COALESCE treats 0 as non-NULL | Use NULLIF(col, 0) before COALESCE |
| 10 | SUM of rates (not ratio-of-sums) | Period rate = cumulative_num / cumulative_denom |
| 11 | Dimension filter gap across layers | Apply same filter at ALL layers |

---

## 8. Performance Patterns

### Early Exit (Never use .count())
```python
# BAD — full data scan just to check existence
if df.count() == 0:  # O(N) — scans entire dataset!
    return

# GOOD — O(1), stops at first row
if df.isEmpty():  # Spark 3.3+
    return

# GOOD — fallback for older Spark
if len(df.head(1)) == 0:
    return
```

### Metadata-Based Freshness (Skip Reprocessing)
```
Gold table is monthly aggregate. Silver gets new daily data.
Instead of reprocessing everything:
  1. Store watermark = max(data_dt) that Gold has already processed
  2. On next run: check if Silver max(data_dt) > watermark
  3. If not → skip (no new data to aggregate)
  4. If yes → process only new partitions, update watermark
```

### Write Strategy Selection
| Platform | Speed | ACID | Schema Evolution | Multi-Engine |
|----------|-------|------|-----------------|--------------|
| Spark Native (Parquet) | ⚡⚡⚡ | ❌ | Manual | Athena, Trino |
| Glue Catalog (saveAsTable) | ⚡ | ❌ | ALTER TABLE | Athena |
| Delta Lake | ⚡⚡ | ✅ | mergeSchema | Spark, Trino* |
| Apache Iceberg | ⚡⚡ | ✅ | Schema evolution | Athena, Spark, Trino, Flink |
| Databricks (Unity Catalog) | ⚡⚡⚡ | ✅ | Managed | Databricks |

**Decision guide:**
- **AWS Glue + Athena only?** → `glue_catalog` or `iceberg` (Athena v3)
- **Need ACID + concurrent writers?** → `delta` or `iceberg`
- **Multi-engine (Spark + Trino + Athena)?** → `iceberg`
- **Databricks stack?** → `databricks` (Unity Catalog, Photon, Liquid Clustering)
- **Maximum raw speed, simple pipelines?** → `spark_native` + deferred catalog sync

### Schema Evolution Strategy
```
Source adds new column → 3 options:
  1. Delta/Iceberg: mergeSchema=true (automatic, safest)
  2. Glue/Hive: ALTER TABLE ADD COLUMNS + rewrite (manual)
  3. Databricks: schema enforcement policy (auto-evolve or reject)

Source removes column → NEVER drop from target (backward compatible):
  - Fill with NULL for new rows
  - Existing data retains historical values
```

### Partition Strategy
```
Best practices:
  - Partition by date (data_dt or mnth_id) for time-series data
  - Aim for 128MB-1GB per partition file
  - Too many small files? → coalesce before write
  - Delta/Iceberg: OPTIMIZE/compact periodically
  - Databricks: Auto Optimize handles this automatically
```

---

## 9. Platform Portability

### If migrating from AWS Glue to Databricks:

| AWS Glue | Databricks Equivalent |
|----------|---------------------|
| Glue Data Catalog | Unity Catalog |
| Glue DynamicFrame | Standard DataFrame |
| S3 Parquet | Delta Tables on S3/ADLS |
| Step Functions | Databricks Workflows |
| EventBridge | Databricks Triggers |
| SageMaker | MLflow + Model Serving |
| Glue DQ | Delta Expectations / Great Expectations |
| getResolvedOptions | dbutils.widgets |

### If migrating from AWS to open-source:

| AWS | Open-Source |
|-----|-------------|
| Glue | Apache Spark (EMR / self-managed) |
| Step Functions | Apache Airflow / Dagster / Prefect |
| SageMaker | MLflow + KubeFlow |
| Feature Store | Feast |
| Glue Catalog | Hive Metastore / Nessie (Iceberg) |
| DynamoDB | PostgreSQL / Redis (for configs) |
| EventBridge | Cron + Airflow Sensors |
