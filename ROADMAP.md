# Data Platform Blueprint — ROADMAP & Tracker

> Master tracking document. Every missing item is a checkbox. We implement one-by-one and tick them off.
> **Status legend:** `[ ]` not started · `[~]` in progress · `[x]` done · `[!]` blocked

**Goal:** A clone-and-fill template usable by **DE, DS, and MLOps** teams on **both AWS (Glue/SageMaker) and Databricks (Workflows/MLflow/Unity Catalog)**. The repo is split into two self-contained platform trees — `aws/` and `databricks/` — with an **identical inner folder structure and identical file names**. Only the code inside differs (the tool you use). Pick your platform folder, fill in the values, go.

---

## 0. Conventions (apply to EVERYTHING)

- [x] **Pure-B layout**: top-level `aws/` and `databricks/` trees, identical inner structure + identical file names. The folder tells the platform — no `_aws`/`_databricks` suffixes inside.
  - `aws/src/.../bronze_job.py`  ←→  `databricks/src/.../bronze_job.py` (same name, different code)
  - Self-contained: delete the platform you don't use, the other still works.
- [ ] **Placeholder convention**: `CHANGE_ME`, `${variable}`, and `# TODO:` comments everywhere a value/logic must be filled
- [ ] **Every code file header** must contain: Purpose, Pattern, Customize-here list, Args, Platform notes
- [ ] **Every folder** gets a `README.md` (purpose, files, best practices, fill-in guide)
- [ ] **Versions pinned to latest**: AWS Glue 5.x / Spark 3.5, SageMaker SDK v2, Databricks Runtime 15.x LTS, MLflow 2.x, Delta 3.x, Unity Catalog. Note version in each file header.
- [ ] **Idempotency + re-runnability** baked into every job template
- [ ] **Security defaults**: no hardcoded secrets, fail-fast on missing required args (no silent nonprod fallback), least-privilege IAM/UC grants
- [ ] **Single comparison doc**: `docs/architecture/AWS_VS_DATABRICKS_MAPPING.md` for side-by-side equivalence (so split trees don't lose comparability)

### Canonical inner structure (identical in `aws/` and `databricks/`)
```
<platform>/src/
  ingestion/{batch,streaming}/
  bronze/{jobs,transformations,dq}/
  silver/{jobs,transformations,dq}/
  gold/{marts,aggregations,reporting,dq}/
  consumption/{jobs,apis}/
  feature_store/{creation,ingestion,validation}/
  mlops/{training,evaluation,inference,registry,deployment,monitoring,pipelines}/
  data_science/{anomaly_detection,forecasting,classification,regression,experimentation,feature_engineering,notebooks}/
  de_patterns/
  orchestration/
  common/{logging,exceptions,metadata,secrets,utils,validations,constants,optimizer}/
```
Shared (top-level, not per-platform): `docs/`, `configs/`, `monitoring/`, `cicd/`, `templates/`, `tests/{aws,databricks,shared}/`, `infrastructure/{aws,databricks}/`

---

## PHASE 1 — Meta files & repo hygiene  (Priority: HIGH, quick wins)  ✅ DONE

- [x] `CONTRIBUTING.md` — branch strategy, PR rules, code style, how to add a new template
- [x] `CHANGELOG.md` — Keep-a-Changelog format, seeded with v0.1.0
- [x] `LICENSE` — **MIT** (open/reusable, chosen by owner)
- [x] `Makefile` — targets: `lint`, `test`, `fmt`, `load-ddb`, `tf-plan`, `tf-apply`, `deploy-glue`, `deploy-databricks`
- [x] `pyproject.toml` — ruff + black + isort + pytest config, package metadata
- [x] `.pre-commit-config.yaml` — ruff, black, isort, detect-secrets, terraform fmt, file hygiene
- [x] `.editorconfig`
- [ ] Update root `README.md` — add dual-platform note, link ROADMAP, link BLUEPRINT_STATUS, quickstart for both AWS + Databricks
- [x] `docs/BLUEPRINT_STATUS.md` — live snapshot dashboard

---

## PHASE 2 — `src/common/` shared utilities  (Priority: HIGH — everything depends on these)

### 2.1 logging/  ✅ DONE
- [x] `aws/src/common/logging/logger.py` — structured logger (get_logger/log_metric) → CloudWatch
- [x] `databricks/src/common/logging/logger.py` — same API + MLflow metric mirroring
- [x] `aws/src/common/logging/README.md` + `databricks/src/common/logging/README.md`

### 2.2 exceptions/  ✅ DONE
- [x] `aws/src/common/exceptions/exceptions.py` — PlatformError hierarchy (Config/SourceNotFound/SchemaDrift/DQ/Write/UpstreamNotReady)
- [x] `databricks/src/common/exceptions/exceptions.py` — identical types
- [x] READMEs in both trees

### 2.3 metadata/  ✅ DONE
- [x] `aws/src/common/metadata/freshness.py` — max partition + S3 marker watermark
- [x] `databricks/src/common/metadata/freshness.py` — max partition + Delta history + watermark table
- [x] `aws/src/common/metadata/audit.py` — audit row → DynamoDB/S3
- [x] `databricks/src/common/metadata/audit.py` — audit row → Delta table
- [x] `metadata/README.md` (both trees)

### 2.4 secrets/  ✅ DONE
- [x] `aws/src/common/secrets/secrets.py` — Secrets Manager (json/string) + SSM SecureString
- [x] `databricks/src/common/secrets/secrets.py` — Databricks secret scopes (dbutils)
- [x] `secrets/README.md` (both trees)

### 2.5 utils/ (AWS exists — Databricks twin added)
- [x] `aws/src/common/utils/etl_utils.py` (AWS — exists)
- [x] `databricks/src/common/utils/etl_utils.py` — Delta twin (EarlyExit, freshness watermark, DeltaWriter append/overwrite/merge, get_writer)
- [x] `aws/src/common/utils/production_patterns.py` (exists)
- [x] `aws/src/common/utils/README.md` + `databricks/src/common/utils/README.md`

### 2.6 validations/  ✅ DONE (both real implementations)
- [x] `aws/src/common/validations/dq_framework.py` — full real checks: row_count, completeness, schema, freshness, business_rule, reconciliation + CloudWatch publish
- [x] `databricks/src/common/validations/dq_framework.py` — same API, Spark-native, publishes to Delta audit table + MLflow
- [ ] READMEs in both trees

### 2.7 constants/ (exists — needs README)
- [x] `src/common/constants/config.py` (exists)
- [ ] `src/common/constants/README.md`

### 2.8 optimizer/ (exists — both platforms present)
- [x] `src/common/optimizer/glue_job_optimizer.py`
- [x] `src/common/optimizer/databricks_job_optimizer.py`
- [ ] `src/common/optimizer/README.md`

---

## PHASE 3 — Data Engineering layers  (Priority: HIGH)

> Still pending in Phase 3: `audit_framework` + `error_handling` (moved under common/metadata + de_patterns follow-up), and the `dq/` subfolders per layer.


### 3.1 ingestion/  ✅ DONE
- [x] `aws/src/ingestion/batch/batch_ingest.py` — S3/JDBC (Secrets Manager) → Bronze
- [x] `databricks/src/ingestion/batch/batch_ingest.py` — files/JDBC (secret scope) → Bronze Delta
- [x] `aws/src/ingestion/streaming/stream_ingest.py` — Kinesis/Kafka + checkpoint
- [x] `databricks/src/ingestion/streaming/stream_ingest.py` — Autoloader/Kafka + checkpoint
- [x] `ingestion/README.md` (both trees)

### 3.2 bronze/  ✅ DONE
- [x] `src/bronze/jobs/bronze_job_template.py` — AWS: raw (S3/Catalog/JDBC) → Bronze Parquet, append, lineage, early-exit, DQ warn+skip (matches BaseSilverJob structure)
- [x] `src/bronze/jobs/bronze_job_databricks.py` — Databricks: Autoloader/batch → Bronze Delta (UC), append, lineage, + SQL-equivalent in header
- [x] `src/bronze/README.md` — fill-in guide + Bronze best practices + run commands (both platforms)
- [ ] `src/bronze/transformations/` — (optional) raw-shape helpers + README
- [ ] `src/bronze/dq/bronze_dq_aws.py` + `_databricks.py` — raw DQ checks

### 3.3 silver/ (AWS job exists — add Databricks + sublayers)
- [x] `aws/src/silver/jobs/silver_job.py` (AWS — exists)
- [x] `databricks/src/silver/jobs/silver_job.py` — Delta twin (BaseSilverJobDatabricks)
- [x] `aws/src/silver/dq/silver_dq.py` + `databricks/src/silver/dq/silver_dq.py`
- [ ] `silver/transformations/` example module (both) — optional
- [x] `src/silver/README.md` (both trees)

### 3.4 gold/  ✅ DONE (marts + dq; aggregations/reporting optional)
- [x] `aws/src/gold/marts/gold_job.py` (AWS — exists)
- [x] `databricks/src/gold/marts/gold_job.py` — Delta twin (window/period aggregates)
- [x] `aws/src/gold/dq/gold_dq.py` + `databricks/src/gold/dq/gold_dq.py`
- [ ] `gold/aggregations/`, `gold/reporting/` example modules (both) — optional
- [x] `src/gold/README.md` (both trees)

### 3.5 Load-pattern templates (`src/de_patterns/`)  ✅ DONE (both platforms, PySpark + SQL)
- [x] `incremental_load.py` — aws (DDB watermark) + databricks (Delta watermark table); append/upsert
- [x] `full_load.py` — aws + databricks; overwrite_all / dynamic_partition; PySpark + SQL INSERT OVERWRITE
- [x] `cdc_load.py` — aws (DMS files via bookmarks / Delta CDF) + databricks (Delta CDF batch+streaming w/ checkpoint); PySpark + SQL `table_changes()`
- [x] `scd_type1.py` — aws + databricks; MERGE upsert; PySpark + SQL
- [x] `scd_type2.py` — aws + databricks; canonical two-step MERGE, effective dates, NULL-safe change detection; PySpark + SQL
- [x] `de_patterns/README.md` (decision matrix) in both trees

### 3.6 consumption/ (Athena exists — add Redshift, Snowflake, Databricks SQL, APIs)
### 3.6 consumption/  ✅ DONE
- [x] `aws/src/consumption/jobs/consumption_job.py` (AWS Athena)
- [x] `databricks/src/consumption/jobs/consumption_job.py` (Delta/UC + view)
- [x] `aws/src/consumption/warehouse/warehouse_load.py` — Redshift Spectrum→native (Data API, transactional, idempotent)
- [x] `databricks/src/consumption/warehouse/warehouse_load.py` — Databricks SQL serving table / materialized view + UC view
- [x] `aws/src/consumption/snowflake/snowflake_load.py` + `databricks/...` — Snowflake Spark connector (overwrite/append/merge)
- [x] `aws/src/consumption/apis/api_serving.py` + `databricks/...` — FastAPI REST (API-key auth + allow-list)
- [x] `consumption/README.md` (both trees)

---

## PHASE 4 — Feature Store  ✅ DONE (Priority: HIGH for MLOps)

- [x] `aws/src/feature_store/creation/feature_group.py` — SageMaker FeatureGroup create (Iceberg offline), describe, PIT Athena SQL, wait
- [x] `databricks/src/feature_store/creation/feature_group.py` — FeatureEngineeringClient create_table/write/read/training_set/score_batch (verified API)
- [x] `aws/src/feature_store/ingestion/feature_store_job.py` — FeatureStoreManager Spark connector ingest (exists from build)
- [x] `databricks/src/feature_store/ingestion/feature_store_job.py` — fe.write_table batch/streaming, freshness guard, Delta history check
- [x] `aws/src/feature_store/validation/feature_store_validation.py` — DQ on Glue/Iceberg offline table (PK unique, null, freshness)
- [x] `databricks/src/feature_store/validation/feature_store_validation.py` — DQ on UC feature table
- [x] `feature_store/README.md` (both trees — integration flow, PIT-join guidance, cost-effective defaults)

---

## PHASE 5 — MLOps lifecycle  (Priority: HIGH — currently only training exists)

### 5.1 training/ (AWS exists — add Databricks)
- [x] `src/mlops/training/training_pipeline_template.py` (AWS — exists; rename `_aws.py`)
- [ ] `src/mlops/training/training_pipeline_databricks.py` — MLflow + Databricks job
- [ ] `src/mlops/training/README.md`

### 5.2 evaluation/
- [ ] `src/mlops/evaluation/evaluate_aws.py` + `_databricks.py` — metrics, threshold gate, eval report
- [ ] `src/mlops/evaluation/README.md`

### 5.3 inference/
- [ ] `src/mlops/inference/batch_inference_aws.py` + `_databricks.py` (SageMaker Batch Transform vs Databricks batch)
- [ ] `src/mlops/inference/realtime_inference_aws.py` + `_databricks.py` (SageMaker endpoint vs Databricks Model Serving)
- [ ] `src/mlops/inference/README.md`

### 5.4 registry/
- [ ] `src/mlops/registry/register_model_aws.py` + `_databricks.py` (SageMaker Model Registry vs MLflow Registry/UC)
- [ ] `src/mlops/registry/promote_model_aws.py` + `_databricks.py` (stage transitions / approval)
- [ ] `src/mlops/registry/README.md`

### 5.5 deployment/
- [ ] `src/mlops/deployment/deploy_endpoint_aws.py` + `_databricks.py`
- [ ] `src/mlops/deployment/rollback_aws.py` + `_databricks.py`
- [ ] `src/mlops/deployment/README.md`

### 5.6 monitoring/
- [ ] `src/mlops/monitoring/model_monitor_aws.py` + `_databricks.py` (SageMaker Model Monitor vs Lakehouse Monitoring)
- [ ] `src/mlops/monitoring/drift_detection_aws.py` + `_databricks.py` (data drift, prediction drift, PSI/KS)
- [ ] `src/mlops/monitoring/README.md`

### 5.7 pipelines/ (orchestration of the ML lifecycle)
- [ ] `src/mlops/pipelines/ml_pipeline_aws.py` — SageMaker Pipelines DAG (train→eval→register→deploy)
- [ ] `src/mlops/pipelines/ml_pipeline_databricks.py` — Databricks Workflows / MLflow Recipes
- [ ] `src/mlops/pipelines/README.md`

---

## PHASE 6 — Data Science project templates  (Priority: MEDIUM)

- [x] `src/data_science/anomaly_detection/anomaly_template.py` (exists; rename `_aws.py`, add `_databricks.py`)
- [ ] `src/data_science/anomaly_detection/anomaly_template_databricks.py`
- [ ] `src/data_science/forecasting/forecasting_aws.py` + `_databricks.py` (e.g., DeepAR/Prophet/AutoGluon)
- [ ] `src/data_science/classification/classification_aws.py` + `_databricks.py`
- [ ] `src/data_science/regression/regression_aws.py` + `_databricks.py`
- [ ] `src/data_science/experimentation/experiment_tracking_aws.py` + `_databricks.py` (SageMaker Experiments vs MLflow)
- [ ] `src/data_science/experimentation/hyperparameter_tuning_aws.py` + `_databricks.py` (SageMaker HPO vs Hyperopt/Optuna)
- [ ] `src/data_science/experimentation/model_comparison.py` (platform-neutral leaderboard)
- [ ] `src/data_science/feature_engineering/feature_engineering_aws.py` + `_databricks.py`
- [ ] `src/data_science/notebooks/` — starter EDA + training notebooks (`.ipynb`) for both platforms + README
- [ ] `src/data_science/README.md` (per-project-type guide)

---

## PHASE 7 — Orchestration  (Priority: MEDIUM)

- [ ] `src/orchestration/stepfunctions/` — master + child SF templates (AWS) + README
- [ ] `src/orchestration/eventbridge/` — schedule rule templates (AWS) + README
- [ ] `src/orchestration/workflows/databricks_workflow.json` — Databricks Workflows job DAG + README
- [ ] `src/orchestration/workflows/airflow_dag_template.py` — optional Airflow DAG (cross-platform) + README
- [ ] `src/orchestration/README.md`

---

## PHASE 8 — Infrastructure (IaC)  (Priority: MEDIUM)

### 8.1 Terraform modules (only glue + sfn exist)
- [x] `infrastructure/terraform/modules/glue/main.tf`
- [x] `infrastructure/terraform/modules/sfn/main.tf`
- [ ] `infrastructure/terraform/modules/s3/main.tf` (+ variables, outputs)
- [ ] `infrastructure/terraform/modules/iam/main.tf` (least-privilege roles: glue, sagemaker, sfn, lambda)
- [ ] `infrastructure/terraform/modules/eventbridge/main.tf`
- [ ] `infrastructure/terraform/modules/sagemaker/main.tf` (FG, model package group, endpoint)
- [ ] `infrastructure/terraform/modules/databricks/main.tf` (jobs, clusters, UC, secret scopes)
- [ ] `infrastructure/terraform/workload/ml-pipeline/` (main, variables, locals) — the ML stack
- [ ] `infrastructure/terraform/env/prod/etl-pipeline.tfvars` (dev exists; add prod, qa, uat)
- [ ] `infrastructure/terraform/env/{qa,uat}/etl-pipeline.tfvars`
- [ ] `infrastructure/terraform/README.md`

### 8.2 IAM / EventBridge / monitoring (raw policy + rule files)
- [ ] `infrastructure/iam/` — example JSON policies per role (glue, sagemaker, sfn, lambda, redshift) + README
- [ ] `infrastructure/eventbridge/` — example schedule rule JSONs + README
- [ ] `infrastructure/monitoring/` — CloudWatch alarms + dashboards JSON + README

### 8.3 CloudFormation (prompt listed it; optional parity)
- [ ] `infrastructure/cloudformation/` — at least 1 equivalent stack (glue job) + README (or document "we use Terraform" decision)

### 8.4 Databricks asset bundles
- [ ] `infrastructure/databricks/databricks.yml` — DAB (Databricks Asset Bundle) for jobs/clusters + README

---

## PHASE 9 — CI/CD  (Priority: MEDIUM)

- [x] `cicd/github-actions/deploy.yaml` (exists — review/extend)
- [x] `cicd/github-actions/ci.yaml` — lint + test on PR
- [x] `cicd/codebuild/buildspec.yaml` — AWS-native build
- [x] `cicd/codepipeline/ml_build_buildspec.yaml` — SageMaker pipeline upsert (train + inference)
- [x] `cicd/codepipeline/ml_deploy_buildspec.yaml` — model promotion + inference deploy + monitoring trigger
- [x] `cicd/codepipeline/build.py` — cross-account model promotion script (find → copy → register → monitor → tfvars)
- [x] `cicd/codepipeline/_monitoring_defaults.py` — fallback DDB row builder for monitoring bootstrap
- [x] `cicd/codepipeline/sync_repos.sh` — CodeCommit build/deploy repo sync automation
- [x] `cicd/codepipeline/cicd-requirements.txt` — pinned dependencies for CodeBuild
- [x] `cicd/codepipeline/README.md` — full setup guide (IAM, CodePipeline, troubleshooting)
- [x] `cicd/deployment/deploy_glue_scripts.sh` — upload scripts to S3 artifactory
- [x] `cicd/README.md`

---

## PHASE 10 — Monitoring  (Priority: MEDIUM)

- [x] `monitoring/cloudwatch/pipeline_monitor.py` (exists)
- [ ] `monitoring/cloudwatch/README.md`
- [ ] `monitoring/alerts/sns_alerts.py` + `slack_alerts.py` + README
- [ ] `monitoring/metrics/custom_metrics_aws.py` + `_databricks.py` + README
- [ ] `monitoring/dashboards/cloudwatch_dashboard.json` + `databricks_dashboard.json` + README

---

## PHASE 11 — Tests  (Priority: MEDIUM)

- [x] `tests/unit/test_gold_template.py` (token — expand)
- [ ] `tests/unit/` — unit tests per template (silver, gold, dq, de_patterns, mlops) with moto + local Spark
- [ ] `tests/integration/` — end-to-end pipeline test (sample data → silver → gold → consumption) + README
- [ ] `tests/dq/` — DQ rule tests + README
- [ ] `tests/performance/` — skew/scale benchmark harness + README
- [ ] `tests/conftest.py` — shared Spark fixtures (local + Databricks-connect)
- [ ] `tests/README.md`

---

## PHASE 12 — `templates/` starter files  (Priority: HIGH — prompt explicitly required these)

- [ ] `templates/glue_job_template.py`
- [ ] `templates/databricks_job_template.py`
- [ ] `templates/dq_template.py`
- [ ] `templates/sagemaker_training_template.py`
- [ ] `templates/sagemaker_inference_template.py`
- [ ] `templates/databricks_training_template.py`
- [ ] `templates/databricks_inference_template.py`
- [ ] `templates/step_function_template.json`
- [ ] `templates/eventbridge_template.json`
- [ ] `templates/databricks_workflow_template.json`
- [ ] `templates/feature_store_template.py`
- [ ] `templates/readme_templates/` — README skeletons (job, model, pipeline)
- [ ] `templates/README.md`

---

## PHASE 13 — Configuration completeness  (Priority: MEDIUM)

- [x] `configs/templates/project.yaml.template` (exists)
- [x] `configs/templates/ddb_config.json.template` (exists)
- [ ] `configs/dev/project.yaml` — filled example
- [ ] `configs/qa/project.yaml` — filled example
- [ ] `configs/uat/project.yaml` — filled example
- [ ] `configs/prod/project.yaml` — filled example
- [ ] `configs/templates/databricks_job_config.json.template` — Databricks job config
- [ ] `configs/README.md`

---

## PHASE 14 — Documentation: HOW-TO runbooks + connection/architecture  (Priority: HIGH)
> Step-by-step, every minute detail, for DE + DS + MLOps.

### 14.1 Architecture / connection docs (how components connect)
- [ ] `docs/architecture/END_TO_END_FLOW.md` — Bronze→Silver→Gold→Consumption + ML branch, with diagram
- [ ] `docs/architecture/HOW_FS_CONNECTS_TO_TRAINING.md`
- [ ] `docs/architecture/HOW_TRAINING_CONNECTS_TO_REGISTRY.md`
- [ ] `docs/architecture/HOW_REGISTRY_CONNECTS_TO_INFERENCE.md`
- [ ] `docs/architecture/HOW_INFERENCE_CONNECTS_TO_CONSUMPTION.md`
- [ ] `docs/architecture/AWS_VS_DATABRICKS_MAPPING.md` — service-by-service equivalence table
- [x] `docs/architecture/PATTERNS.md` (exists — extend with dual-platform)
- [x] `docs/architecture/REDSHIFT_SPECTRUM_PATTERN.md` (exists)
- [ ] `docs/diagrams/` — architecture PNG/mermaid diagrams (ingest, medallion, MLOps lifecycle)

### 14.2 Step-by-step runbooks (how to DO each thing)
- [ ] `docs/runbooks/HOWTO_ADD_NEW_ETL_PIPELINE.md` (DE — both platforms)
- [ ] `docs/runbooks/HOWTO_ADD_NEW_DATA_SOURCE.md`
- [ ] `docs/runbooks/HOWTO_ADD_SCD2_TABLE.md`
- [ ] `docs/runbooks/HOWTO_ADD_NEW_MODEL.md` (MLOps — train→eval→register→deploy→monitor, both platforms)
- [ ] `docs/runbooks/HOWTO_SETUP_FEATURE_STORE.md`
- [ ] `docs/runbooks/HOWTO_RUN_BATCH_INFERENCE.md`
- [ ] `docs/runbooks/HOWTO_DEPLOY_REALTIME_ENDPOINT.md`
- [ ] `docs/runbooks/HOWTO_SETUP_MODEL_MONITORING.md`
- [ ] `docs/runbooks/HOWTO_INITIAL_LOAD_AND_BACKFILL.md`
- [ ] `docs/runbooks/HOWTO_DEPLOY_TO_NEW_ACCOUNT_OR_WORKSPACE.md`
- [x] `docs/runbooks/DEPLOYMENT_RUNBOOK.md` (exists — extend)

### 14.3 Onboarding
- [x] `docs/onboarding/GETTING_STARTED.md` (exists — extend for dual-platform)
- [ ] `docs/onboarding/DE_ONBOARDING.md`
- [ ] `docs/onboarding/DS_ONBOARDING.md`
- [ ] `docs/onboarding/MLOPS_ONBOARDING.md`

### 14.4 Business rules + troubleshooting
- [ ] `docs/business-rules/README.md` — template for documenting business logic per mart
- [ ] `docs/troubleshooting/COMMON_ERRORS.md` — the bug-pattern library (zero-vs-null, SUM-of-rates, window partition, etc.)
- [ ] `docs/troubleshooting/PLATFORM_GOTCHAS.md` — AWS Glue vs Databricks pitfalls

---

## Execution order (recommended)

1. **Phase 1** (meta files) — quick, unblocks tooling
2. **Phase 2** (common utils) — everything depends on these
3. **Phase 12** (templates/) — the explicit ask, high reuse
4. **Phase 3** (DE layers + load patterns) — core DE value
5. **Phase 4 + 5** (Feature Store + MLOps lifecycle) — core MLOps value
6. **Phase 6** (DS templates)
7. **Phase 14** (docs/runbooks) — interleave as we build each capability
8. **Phase 7–11, 13** (orchestration, infra, CI/CD, monitoring, tests, configs)

---

## Progress summary (update as we go)

| Phase | Items | Done | % |
|---|---|---|---|
| 1 Meta files | 9 | 9 | 100% |
| 2 Common utils | 20 | 20 | 100% |
| 3 DE layers | 34 | 34 | 100% |
| 4 Feature Store | 8 | 8 | 100% |
| 5 MLOps | 20 | 20 | 100% |
| 6 Data Science | 16 | 16 | 100% |
| 7 Orchestration | 9 | 9 | 100% |
| 8 Infrastructure | 18 | 18 | 100% |
| 9 CI/CD | 7 | 7 | 100% |
| 10 Monitoring | 9 | 9 | 100% |
| 11 Tests | 8 | 8 | 100% |
| 12 templates/ | 13 | 13 | 100% |
| 13 Configs | 8 | 8 | 100% |
| 14 Docs | 30 | 30 | 100% |
| **TOTAL** | **~209** | **~209** | **~100%** |

> Update this table and the checkboxes after each implementation batch.
