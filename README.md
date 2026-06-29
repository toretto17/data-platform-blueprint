# Data Platform Blueprint

> **A production-grade, reusable blueprint for building data platforms on AWS (Glue / SageMaker) and Databricks (Workflows / MLflow / Unity Catalog).** Clone it, fill in the placeholders, start building. DE, DS, and MLOps — all in one repo.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Quick Start

```bash
# 1. Clone
git clone https://github.com/toretto17/data-platform-blueprint.git my-project && cd my-project
git clone <this-repo> my-project && cd my-project

# 2. Pick your platform
ls aws/     # ← if you're on AWS (Glue / SageMaker)
ls databricks/  # ← if you're on Databricks

# 3. Configure
cp configs/templates/project.yaml.template configs/dev/project.yaml
# Edit with your account_id, buckets, roles

# 4. Start building
# Copy any template → fill CHANGE_ME placeholders → run
```

## Repository structure

```
├── aws/                      ← Self-contained AWS tree (Glue + SageMaker + boto3)
│   └── src/{ingestion, bronze, silver, gold, consumption, de_patterns,
│            feature_store, mlops, data_science, orchestration, common}/
│
├── databricks/               ← Self-contained Databricks tree (Delta + UC + MLflow)
│   └── src/  (IDENTICAL structure — same file names, different code)
│
├── infrastructure/           ← IaC (Terraform modules + SF JSONs + Lambda)
│   ├── aws/
│   └── databricks/
│
├── configs/                  ← Environment configs (dev/qa/uat/prod)
├── templates/                ← Starter files (pointers to full implementations)
├── tests/                    ← Unit + integration tests (per platform)
├── docs/                     ← Architecture, runbooks, getting started
├── cicd/                     ← GitHub Actions + CodeBuild pipelines
└── monitoring/               ← CloudWatch / alerts
```

**Key design:** `aws/` and `databricks/` have **identical inner folder structures and file names**. Pick your platform, delete the other, go. No cross-tree dependencies.

## What's inside (86 Python files, both platforms)

### Data Engineering
| Capability | File (same name in both trees) |
|---|---|
| Batch ingestion (S3/JDBC/files) | `src/ingestion/batch/batch_ingest.py` |
| Streaming (Kinesis/Kafka/Autoloader) | `src/ingestion/streaming/stream_ingest.py` |
| Bronze (raw landing, lineage, append) | `src/bronze/jobs/bronze_job.py` |
| Silver (cleanse, dedup, DQ) | `src/silver/jobs/silver_job.py` |
| Gold (aggregations, MTD/YTD, windows) | `src/gold/marts/gold_job.py` |
| Consumption (warehouse, Snowflake, API) | `src/consumption/{jobs,warehouse,snowflake,apis}/` |
| **CDC / CDF** | `src/de_patterns/cdc_load.py` |
| **SCD Type 1** (overwrite) | `src/de_patterns/scd_type1.py` |
| **SCD Type 2** (history, effective dates) | `src/de_patterns/scd_type2.py` |
| Incremental load (watermark) | `src/de_patterns/incremental_load.py` |
| Full load (snapshot overwrite) | `src/de_patterns/full_load.py` |

### MLOps (full lifecycle)
| Stage | File |
|---|---|
| Training (+Feature Store) | `src/mlops/training/training_pipeline.py` |
| Evaluation (threshold gate) | `src/mlops/evaluation/evaluate.py` |
| Batch + Realtime inference | `src/mlops/inference/inference.py` |
| Model registry + promotion | `src/mlops/registry/registry.py` |
| Deployment (canary + rollback) | `src/mlops/deployment/deployment.py` |
| Monitoring + drift detection | `src/mlops/monitoring/monitoring.py` |
| End-to-end pipeline orchestration | `src/mlops/pipelines/ml_pipeline.py` |

### Data Science
| Domain | File |
|---|---|
| Forecasting (Prophet, LightGBM) | `src/data_science/forecasting/forecasting.py` |
| Classification (Optuna HPO, SHAP) | `src/data_science/classification/classification.py` |
| Regression (model comparison) | `src/data_science/regression/regression.py` |
| Anomaly Detection (IsolationForest) | `src/data_science/anomaly_detection/anomaly_detection.py` |
| Experiment tracking + HPO | `src/data_science/experimentation/experimentation.py` |
| Feature engineering (lags, rolling) | `src/data_science/feature_engineering/feature_engineering.py` |

### Shared utilities (per platform)
| Module | Purpose |
|---|---|
| `common/logging/` | Structured logger (CloudWatch / driver logs + MLflow) |
| `common/exceptions/` | Typed error hierarchy |
| `common/metadata/` | Audit trail + freshness guard |
| `common/secrets/` | Secrets Manager / Databricks secret scopes |
| `common/utils/` | Early-exit, write strategies, production patterns |
| `common/validations/` | DQ framework (6 checks + metrics publishing) |
| `common/optimizer/` | Job performance optimizer |

## Platform comparison (AWS ↔ Databricks)

| Concept | AWS | Databricks |
|---|---|---|
| ETL engine | Glue (Spark) | Databricks Runtime (Spark) |
| Catalog | Glue Data Catalog | Unity Catalog |
| Table format | Parquet / Delta / Iceberg | Delta Lake |
| Feature Store | SageMaker FeatureGroup | UC feature table (any Delta + PK) |
| Model registry | SageMaker Model Packages | MLflow + UC aliases |
| Model serving | SageMaker Endpoint / Serverless | Model Serving (scale_to_zero) |
| Monitoring | Model Monitor + CloudWatch | Lakehouse Monitor + MLflow |
| Orchestration | Step Functions + EventBridge | Databricks Workflows |
| CDC | DMS + Job Bookmarks / Delta CDF | Delta Change Data Feed |
| HPO | Optuna (portable) | Optuna (recommended over Hyperopt) |

## How to use this template

1. **Clone the repo**
2. **Delete the platform you don't use** (`rm -rf databricks/` or `rm -rf aws/`)
3. **Fill `configs/<env>/project.yaml`** with your account, buckets, roles
4. **Copy a template file** (e.g. `src/silver/jobs/silver_job.py`) into your project
5. **Search for `CHANGE_ME`** — fill every placeholder with your values
6. **Override the methods** marked `raise NotImplementedError` (your business logic)
7. **Run `make lint test`** to validate
8. **Deploy** with Terraform (`make tf-apply`) or Databricks Asset Bundles

## Configuration

All environment-specific values live in `configs/<env>/project.yaml`. DDB job configs use `${environment}` / `${account_id}` placeholders substituted at deploy time. See `configs/README.md`.

## Best practices enforced

- ✅ **No hardcoded secrets** — fail-fast on missing args (never silent nonprod fallback)
- ✅ **Idempotent** — MERGE/upsert, dynamic partition overwrite, freshness guards
- ✅ **Cost-effective** — early-exit, scale_to_zero, spot instances, AQE, skip-if-fresh
- ✅ **DQ at every layer** — warn+skip pattern (never crash on missing rulesets)
- ✅ **No future leakage** — temporal splits, window ROWS PRECEDING only
- ✅ **Round floats** before write (2dp)
- ✅ **Optuna** for HPO (not deprecated Hyperopt)
- ✅ **CDC via Delta CDF** (Databricks) / DMS + Job Bookmarks (AWS)
- ✅ **SCD2 two-step MERGE** (mergeKey trick, NULL-safe `<=>`)
- ✅ **Production bug-patterns** documented and guarded in code

## Tracking

See [`ROADMAP.md`](ROADMAP.md) for the full task tracker and [`docs/BLUEPRINT_STATUS.md`](docs/BLUEPRINT_STATUS.md) for the maturity dashboard.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for conventions, PR workflow, and how to add new templates.

## License

[MIT](LICENSE) — open and reusable.
