# 📊 Blueprint Status — Snapshot Dashboard

> Quick health check of the enterprise template. For the full task list see [`../ROADMAP.md`](../ROADMAP.md).
> Last updated: 2026-07-10

---

## Vision

A clone-and-fill enterprise data platform template that works on **AWS** and **Databricks**, usable by **DE, DS, and MLOps** teams. Every capability ships as two files (one per platform) with identical structure and READMEs explaining what to fill in.

---

## Current maturity

| Dimension | Status | Notes |
|---|---|---|
| Folder skeleton | 🟢 Complete | `aws/` and `databricks/` with identical inner structure |
| AWS Glue DE templates | 🟢 Complete | Ingestion, Bronze→Silver→Gold→Consumption, load patterns (CDC/SCD1/2/incremental/full), DQ, warehouse/Snowflake/API |
| Databricks DE templates | 🟢 Complete | Full medallion + load patterns + DQ + warehouse/Snowflake/API twins (Delta/UC/Autoloader/CDF) |
| MLOps lifecycle | 🟢 Complete | All 7 stages: Train → Eval → Register → Inference → Deploy → Monitor → Pipelines |
| Data Science templates | 🟢 Complete | Forecasting, Classification, Regression, Anomaly Detection, Experimentation, Feature Engineering |
| Feature Store | 🟢 Complete | Create, ingest, validate (SageMaker FS / UC Feature Engineering) |
| Meta files | 🟢 Complete | CONTRIBUTING, CHANGELOG, LICENSE, Makefile, pyproject.toml, .pre-commit-config, .editorconfig |
| `templates/` starters | 🟢 Complete | 14 starter files (Glue, Databricks, DQ, SageMaker, SF, EventBridge, Workflow, Feature Store, READMEs) |
| How-to runbooks | 🟢 Complete | 20 step-by-step runbooks + 3 onboarding guides (DE, DS, MLOps) |
| Tests | 🟢 Complete | conftest.py + unit tests (DQ, ETL utils, normalization) + integration skeletons |
| Infra (Terraform) | 🟢 Complete | S3, IAM, EventBridge modules + env tfvars + Databricks Asset Bundles |
| Configs (env examples) | 🟢 Complete | dev/qa/uat/prod project.yaml + DDB config templates + load script |
| CI/CD (ETL) | 🟢 Complete | GitHub Actions (lint+test+deploy), CodeBuild buildspec, deploy scripts |
| CI/CD (ML) | 🟢 Complete | CodePipeline templates: build (pipeline upsert) + deploy (model promotion + monitoring) + sync |
| Monitoring | 🟢 Complete | Alerts (SNS+Slack), CloudWatch dashboard, custom metrics |
| Orchestration | 🟢 Complete | Step Functions + EventBridge (AWS), Databricks Workflows (DBX), Airflow DAG template |
| Documentation | 🟢 Complete | Architecture docs, AWS vs DBX mapping, troubleshooting, platform gotchas, 66 folder READMEs |
| Community standards | 🟢 Complete | Code of Conduct, Security Policy, Issue/PR templates |

🟢 done · 🟡 partial · 🔴 missing

---

## Overall completion: 100% (209 of 209 tracked items)

**Total files:** 290+  
**Python files:** 88  
**Markdown docs:** 130+  
**All Python files:** Syntax-checked (`py_compile`)  
**Static analysis:** Clean on critical rules (`ruff --select F821,F811,F601,F402,B006,E722,F823`)

---

## What's included (all production-ready, CHANGE_ME placeholders)

### Data Engineering
- Batch + streaming ingestion
- Full medallion (Bronze → Silver → Gold → Consumption)
- 5 load patterns (Incremental, Full, CDC/CDF, SCD Type 1, SCD Type 2)
- Data Quality framework (6 checks, warn+skip)
- Warehouse loading (Redshift Spectrum, Snowflake MERGE, DBSQL)
- REST API serving (FastAPI + sqlglot SQL injection prevention)
- `DataOptimizer` (file sizing, skew detection, salting)
- `EarlyExitCheck` + `MetadataFreshnessManager`

### MLOps
- Full lifecycle (7 stages) for both platforms
- SageMaker Pipelines (@step decorator) / Databricks Workflows
- Model Registry (Package Groups / UC aliases)
- Batch + realtime inference (scale-to-zero)
- Canary deployment + rollback
- Model monitoring (PSI/KS drift, DQ baselines)
- Feature Store (create, ingest, validate, PIT)

### ML CI/CD
- CodePipeline + CodeBuild templates (build + deploy)
- Model promotion script (`build.py`) — cross-account, with monitoring DDB mirroring
- Monitoring defaults fallback (`_monitoring_defaults.py`)
- CodeCommit sync automation (`sync_repos.sh`)
- GitHub Actions for ETL (lint → test → deploy → Terraform)

### Data Science
- 6 domains: Forecasting, Classification, Regression, Anomaly, Experimentation, Feature Engineering
- Optuna HPO (not deprecated Hyperopt)
- SHAP explainability
- Model comparison leaderboard

### Infrastructure
- Terraform modules (S3, IAM, EventBridge)
- Databricks Asset Bundles
- Environment-specific configurations (dev/qa/uat/prod)

---

## Platform parity check

```bash
# Verify identical directory structure:
diff <(cd aws && find . -type d | sort) <(cd databricks && find . -type d | sort)
# Should return empty (identical)
```
