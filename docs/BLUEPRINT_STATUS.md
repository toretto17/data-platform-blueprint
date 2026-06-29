# Blueprint Status — Snapshot Dashboard

> Quick health check of the enterprise template. For the full task list see [`../ROADMAP.md`](../ROADMAP.md).
> Last updated: 2026-06-28

---

## Vision

A clone-and-fill enterprise data platform template that works on **AWS** and **Databricks**, usable by **DE, DS, and MLOps** teams. Every capability ships as two files (`*_aws.py` + `*_databricks.py`) with a README explaining what to fill in.

---

## Current maturity

| Dimension | Status | Notes |
|---|---|---|
| Folder skeleton | 🟢 Complete | Matches target structure |
| AWS Glue DE templates | 🟢 Complete | Ingestion, Bronze→Silver→Gold→Consumption, load patterns (CDC/SCD1/2/incremental/full), DQ, warehouse/Snowflake/API |
| Databricks DE templates | 🟢 Complete | Full medallion + load patterns + DQ + warehouse/Snowflake/API twins (Delta/UC/Autoloader/CDF) |
| MLOps lifecycle | 🔴 Minimal | Only training exists (1 of 7 stages) |
| Data Science templates | 🔴 Minimal | Only anomaly exists |
| Meta files | 🔴 Missing | No CONTRIBUTING/CHANGELOG/LICENSE/Makefile/pyproject/pre-commit |
| `templates/` starters | 🔴 Empty | Prompt explicitly required these |
| How-to runbooks | 🔴 Minimal | Connection + step-by-step docs needed |
| Tests | 🔴 Token | 1 file, doesn't exercise templates |
| Infra (Terraform) | 🟡 Partial | glue + sfn modules only |
| Configs (env examples) | 🟡 Partial | templates exist; no filled env examples |

🟢 done · 🟡 partial · 🔴 missing

---

## Overall completion: ~11% (≈24 of ≈209 tracked items)

---

## What works TODAY (a developer can use right now)

- AWS Glue Silver / Gold / Consumption / Feature Store job templates
- Production-extracted bug patterns + DQ warn-skip framework
- DDB-driven config + Lambda config-loader + 3 real Step Functions examples
- Glue + Databricks job optimizers
- 859-line ETL utils (early-exit, write strategies, partition mgmt)

## What's NOT usable yet

- Anything on Databricks (no `_databricks.py` twins beyond the optimizer)
- Bronze layer, CDC, SCD1/2, incremental/full load patterns
- 6 of 7 MLOps stages (eval, inference, registry, deploy, monitor, pipelines)
- Forecasting / classification / regression DS templates
- The `templates/` starter folder
- End-to-end how-to runbooks

---

## Immediate next batch

Per ROADMAP execution order:
1. Phase 1 — meta files (CONTRIBUTING, CHANGELOG, LICENSE, Makefile, pyproject, pre-commit)
2. Phase 2 — `src/common/` shared utils (logging, exceptions, metadata, secrets, dual-platform)
3. Phase 12 — `templates/` starter files

---

## Conventions reminder

- 1 capability = 2 files: `*_aws.py` + `*_databricks.py`
- Every folder has a `README.md`
- Placeholders: `CHANGE_ME`, `${var}`, `# TODO:`
- Fail-fast on missing required args (no silent nonprod fallback)
- Pin to latest versions (Glue 5.x / Spark 3.5, SageMaker SDK v2, DBR 15.x LTS, MLflow 2.x, Delta 3.x, Unity Catalog)
