# Databricks ML CI/CD — Asset Bundles + UC Model Registry

## Overview

Production-grade ML CI/CD for Databricks using:
- **Declarative Automation Bundles** (formerly DABs) for workflow deployment
- **Unity Catalog** for model registry (Champion/Challenger aliases)
- **Lakehouse Monitor** for model quality tracking
- **GitHub Actions** for CI/CD automation

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     GitHub Repository                                 │
│                                                                       │
│  PR → validate bundle + lint + test                                  │
│  merge to dev → deploy to dev workspace                              │
│  merge to main → deploy staging (auto) → deploy prod (approval)     │
└───────┬──────────────────────────┬───────────────────────┬──────────┘
        │                          │                       │
        ▼                          ▼                       ▼
┌───────────────┐     ┌─────────────────┐     ┌────────────────┐
│  Dev Workspace │     │ Staging Workspace│     │ Prod Workspace  │
│               │     │                 │     │                │
│  Training ✓  │     │  Training ✓    │     │  Inference ✓  │
│  Inference ✓ │     │  Inference ✓   │     │  Monitoring ✓ │
│  Experiment  │     │  Validation ✓  │     │  Champion model│
│               │     │                 │     │                │
│  UC: dev_cat  │     │  UC: stg_cat   │     │  UC: prod_cat  │
└───────────────┘     └─────────────────┘     └────────────────┘
```

### Model Promotion Flow

```
Train in Dev/Staging → Register → Assign "Challenger" alias
         │
         ▼
Validate (quality gates: accuracy > 0.85, rmse < 0.15)
         │
         ▼ (passes)
promote_model.py → Move alias: Challenger → Champion
         │
         ▼
Inference jobs load model@Champion → automatically use new version
```

---

## Files

| File | Purpose | When Used |
|------|---------|-----------|
| `ml_bundle.yml` | Asset Bundle: defines training, inference, monitoring jobs | `databricks bundle deploy` |
| `promote_model.py` | Promotes Challenger → Champion in UC (with validation gates) | After training validation passes |
| `ml_deploy.yaml` | GitHub Actions: validate → deploy dev → staging → prod | On every push/merge |
| `README.md` | This documentation | Reference |

---

## Quick Start

### 1. Configure your bundle

Edit `ml_bundle.yml`:
- Replace all `CHANGE_ME` values
- Set workspace URLs in `targets`
- Set Unity Catalog names
- Set notebook paths

### 2. Validate locally

```bash
# Install Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Set credentials
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...

# Validate
cd cicd/databricks
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev

# Run training manually
databricks bundle run model_training_job -t dev
```

### 3. Set up GitHub secrets

| Secret | Value |
|--------|-------|
| `DATABRICKS_HOST_DEV` | `https://your-dev.cloud.databricks.com` |
| `DATABRICKS_TOKEN_DEV` | Service principal token for dev |
| `DATABRICKS_HOST_STAGING` | `https://your-staging.cloud.databricks.com` |
| `DATABRICKS_TOKEN_STAGING` | Service principal token for staging |
| `DATABRICKS_HOST_PROD` | `https://your-prod.cloud.databricks.com` |
| `DATABRICKS_TOKEN_PROD` | Service principal token for prod |

### 4. Push and deploy

```bash
git push origin dev    # → deploys to dev workspace
git push origin main   # → deploys to staging, then prod (after approval)
```

---

## Model Lifecycle

### 1. Training (scheduled or triggered)

```
model_training_job:
  prepare_features → train_model → validate_model → register_model
```

- New model version registered in UC with "Challenger" alias
- Metrics logged to MLflow experiment

### 2. Validation

`validate_model` task checks:
- Accuracy ≥ threshold
- RMSE ≤ threshold
- No data drift vs baseline
- Comparison with current Champion

### 3. Promotion (Challenger → Champion)

```bash
# Manual promotion (after human review):
python promote_model.py --model-name catalog.schema.model --from-alias Challenger --to-alias Champion

# Or trigger via GitHub Actions workflow_dispatch
# Or auto-promote in CI/CD if validation passes (remove manual approval)
```

### 4. Inference (uses Champion)

```python
# In inference notebook:
import mlflow
model = mlflow.pyfunc.load_model(f"models:/{model_name}@Champion")
predictions = model.predict(input_df)
```

### 5. Monitoring

Lakehouse Monitor tracks:
- Data quality (schema changes, NULL rates, distribution shifts)
- Model quality (prediction accuracy vs ground truth)
- PSI/KS drift statistics

If violations detected → can auto-trigger retraining job.

---

## Bundle Commands Reference

```bash
# Validate configuration
databricks bundle validate -t dev

# Deploy (create/update jobs, experiments, models)
databricks bundle deploy -t dev

# Run a specific job
databricks bundle run model_training_job -t dev
databricks bundle run batch_inference_job -t prod

# List deployed resources
databricks bundle summary -t dev

# Destroy (remove all resources)
databricks bundle destroy -t dev

# View deployment status
databricks jobs list --output json
```

---

## Unity Catalog Model Aliases

| Alias | Meaning | Who assigns | Who uses |
|-------|---------|-------------|----------|
| **Champion** | Production model | `promote_model.py` | Inference jobs (`@Champion`) |
| **Challenger** | Candidate under evaluation | Training job | Validation job |

### Key API calls (MLflow 2.9+):

```python
from mlflow import MlflowClient

client = MlflowClient()

# Assign alias
client.set_registered_model_alias(name="catalog.schema.model", alias="Champion", version="5")

# Load by alias
import mlflow
model = mlflow.pyfunc.load_model("models:/catalog.schema.model@Champion")

# Get version by alias
mv = client.get_model_version_by_alias(name="catalog.schema.model", alias="Champion")
print(mv.version)  # "5"

# Remove alias
client.delete_registered_model_alias(name="catalog.schema.model", alias="Challenger")
```

---

## Comparison: AWS (CodePipeline) vs Databricks (Asset Bundles)

| Aspect | AWS CodePipeline | Databricks Asset Bundles |
|--------|-----------------|--------------------------|
| Deployment unit | S3 scripts + DDB configs + Terraform | YAML bundle (`databricks bundle deploy`) |
| Model Registry | SageMaker Model Package Groups | Unity Catalog + MLflow aliases |
| Promotion | `build.py` (copy artifacts cross-account) | `promote_model.py` (set alias) |
| Monitoring | SageMaker Model Monitor + SF trigger | Lakehouse Monitor (API/SQL) |
| CI/CD | CodeBuild buildspecs | GitHub Actions + `databricks` CLI |
| Environments | Account isolation (nonprod/prod) | Workspace isolation (dev/staging/prod) |
| Approval | CodePipeline manual approval | GitHub environment protection |
| Inference | Batch Transform / Serverless endpoint | Databricks Workflows / Model Serving |

---

## Troubleshooting

### "Bundle validation failed"
- Check YAML syntax: `databricks bundle validate -t dev`
- Verify all `notebook_path` entries point to existing files
- Ensure Spark version is valid: `databricks clusters spark-versions`

### "Permission denied on model"
- Service Principal needs MANAGE permission on UC model
- Verify: `GRANT MANAGE ON REGISTERED MODEL catalog.schema.model TO \`sp-name\``

### "Alias not found"
- Model version hasn't been registered yet
- Run training job first to create initial version with "Challenger" alias

### "Lakehouse Monitor refresh fails"
- Table must be Delta format in Unity Catalog
- Enable CDF on monitored table: `ALTER TABLE ... SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')`
- SP must own the table or have appropriate permissions

### "Bundle deploy shows drift"
- Another user modified jobs via UI → run `databricks bundle deploy -t dev` to reconcile
- Use `--force` flag if needed (overwrites manual changes)

---

## Security Best Practices

- Use **Service Principals** (not personal tokens) for CI/CD
- Store tokens in GitHub **encrypted secrets** (never in code)
- Enable **Unity Catalog** for centralized access control
- Use **separate catalogs** per environment (dev/staging/prod)
- Enable **audit logging** for model registry changes
- Set **IP access lists** on production workspace
- Use **OIDC federation** with GitHub for tokenless auth (Databricks 2025+)
