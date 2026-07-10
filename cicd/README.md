# CI/CD — Complete Guide

## What This Is

This directory contains **everything you need to automate deployment** of your data platform — both the ETL pipelines AND the ML model lifecycle. Two platforms are supported:

- **AWS** — Glue ETL scripts + SageMaker ML models (via CodePipeline + CodeBuild)
- **Databricks** — Delta ETL + Unity Catalog ML models (via Asset Bundles + GitHub Actions)

If you don't know where to start, read the section for your platform below.

---

## Directory Structure

```
cicd/
│
├── README.md                          ← YOU ARE HERE (start guide)
│
├── github-actions/                    ← ETL deployment (both platforms)
│   ├── ci.yaml                        Lint + test on every PR
│   └── deploy.yaml                    Deploy ETL scripts + Terraform (dev/prod)
│
├── codebuild/                         ← AWS ETL alternative (CodeBuild)
│   └── buildspec.yaml                 Lint + test + deploy Glue scripts
│
├── codepipeline/                      ← AWS ML CI/CD (SageMaker)
│   ├── README.md                      Full setup guide for AWS ML
│   ├── ml_build_buildspec.yaml        Upserts SageMaker Training + Inference pipelines
│   ├── ml_deploy_buildspec.yaml       Promotes models cross-account + monitoring
│   ├── build.py                       Model promotion script (23KB)
│   ├── _monitoring_defaults.py        Fallback monitoring config builder
│   ├── sync_repos.sh                  Syncs code to CodeCommit repos
│   └── cicd-requirements.txt          Python dependencies for CodeBuild
│
├── databricks/                        ← Databricks ML CI/CD (Asset Bundles + UC)
│   ├── README.md                      Full setup guide for Databricks ML
│   ├── ml_bundle.yml                  Asset Bundle: training + inference + monitoring jobs
│   ├── promote_model.py               Champion/Challenger promotion (UC aliases)
│   └── ml_deploy.yaml                 GitHub Actions for Databricks ML deploy
│
└── deployment/                        ← Shared deployment scripts
    └── deploy_glue_scripts.sh         Upload ETL scripts to S3
```

---

## I'm Using AWS (Glue + SageMaker)

### For ETL Only (Glue scripts):

1. Copy `github-actions/deploy.yaml` to `.github/workflows/deploy.yaml`
2. Set GitHub secrets: `AWS_DEPLOY_ROLE_DEV`, `AWS_DEPLOY_ROLE_PROD`, `AWS_ACCOUNT_ID_DEV`, `AWS_ACCOUNT_ID_PROD`
3. Push to `dev` branch → scripts upload to S3 + Terraform applies

**Or** use CodeBuild: copy `codebuild/buildspec.yaml` to your CodeBuild project.

### For ML Models (SageMaker):

**Read:** [`codepipeline/README.md`](codepipeline/README.md) — full step-by-step guide.

**Summary:**
1. Create CodeCommit repos (1 BUILD + 1 DEPLOY per model family)
2. Create CodeBuild projects using the buildspecs
3. Create CodePipeline (Source → Approval → Deploy)
4. Run `sync_repos.sh` to sync your code → CodeCommit
5. CodePipeline auto-triggers on push

**What happens:**
- BUILD triggers → SageMaker Pipelines (train + inference) created/updated
- Models train → get Approved in SageMaker Studio
- DEPLOY triggers → model promoted to prod → monitoring configured

---

## I'm Using Databricks (Delta + Unity Catalog)

### For ETL Only (Delta scripts):

1. Copy `github-actions/deploy.yaml` to `.github/workflows/deploy.yaml`
2. Set GitHub secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
3. Push to `dev` → Asset Bundle deploys ETL jobs to workspace

### For ML Models (UC + MLflow):

**Read:** [`databricks/README.md`](databricks/README.md) — full step-by-step guide.

**Summary:**
1. Edit `ml_bundle.yml` — set workspace URLs, catalog names, notebook paths
2. Create Service Principals per workspace (dev/staging/prod)
3. Store tokens as GitHub secrets
4. Copy `databricks/ml_deploy.yaml` to `.github/workflows/`
5. Push to `dev` or `main` → workflows deploy automatically

**What happens:**
- Push to dev → bundle deploys (training + inference + monitoring jobs)
- Training runs → model registered as "Challenger"
- Validation passes → `promote_model.py` moves alias to "Champion"
- Inference jobs auto-load `@Champion` → new model live

---

## I'm Using Both (AWS + Databricks)

Use AWS CI/CD for SageMaker models AND Databricks CI/CD for UC models.
They are completely independent — no conflicts.

---

## Quick Reference: What Does What

| I want to... | File to use | Platform |
|---|---|---|
| Deploy ETL scripts on git push | `github-actions/deploy.yaml` | Both |
| Run lint + test on PR | `github-actions/ci.yaml` | Both |
| Upsert SageMaker Pipelines | `codepipeline/ml_build_buildspec.yaml` | AWS |
| Promote SageMaker model to prod | `codepipeline/ml_deploy_buildspec.yaml` + `build.py` | AWS |
| Deploy Databricks ML workflows | `databricks/ml_bundle.yml` + `ml_deploy.yaml` | Databricks |
| Promote UC model (Champion) | `databricks/promote_model.py` | Databricks |
| Upload Glue scripts to S3 manually | `deployment/deploy_glue_scripts.sh` | AWS |
| Sync code to CodeCommit | `codepipeline/sync_repos.sh` | AWS |
| Configure monitoring (AWS) | `codepipeline/_monitoring_defaults.py` | AWS |
| Configure monitoring (DBX) | `databricks/ml_bundle.yml` (monitoring_job) | Databricks |

---

## How Environments Work

### AWS (CodePipeline):

| Environment | Account | Trigger |
|---|---|---|
| dev/nonprod | `NONPROD_ACCOUNT_ID` | Push to BUILD CodeCommit repo |
| prod | `PROD_ACCOUNT_ID` | Manual approval in CodePipeline |

### Databricks (Asset Bundles):

| Environment | Workspace | Trigger |
|---|---|---|
| dev | Dev workspace | Push to `dev` branch |
| staging | Staging workspace | Merge to `main` (auto) |
| prod | Prod workspace | Merge to `main` (after GitHub approval) |
| test | Staging workspace | PR (integration tests) |

---

## Security

- **Never hardcode credentials** — use GitHub secrets / CodeBuild env vars / Service Principals
- **Use separate accounts/workspaces** per environment
- **Require approval** before prod deployment (CodePipeline approval / GitHub environment protection)
- **Service Principals** for automation (not personal tokens)
- **Least privilege** — CI/CD roles only have permissions they need
- **Audit trail** — all deployments logged (CodePipeline history / GitHub Actions logs / Databricks audit)

---

## Choosing Your CI/CD Approach

| Criterion | GitHub Actions | CodePipeline (AWS) | Asset Bundles (DBX) |
|---|---|---|---|
| Best for | ETL + generic deploy | SageMaker model lifecycle | Databricks ML lifecycle |
| Trigger | Git push to GitHub | Push to CodeCommit | `databricks bundle deploy` or GitHub Actions |
| Cross-account | OIDC role assumption | sts:AssumeRole | Workspace isolation |
| Model promotion | N/A | `build.py` (copy artifacts) | `promote_model.py` (set alias) |
| Approval gate | GitHub environments | CodePipeline stage | GitHub environments |
| Monitoring setup | N/A | SF trigger per model | Lakehouse Monitor (in bundle) |
| Container build | Docker action | ECR push in buildspec | N/A (built-in runtimes) |
| Cost | Free (public) / included | Per execution | Free (Databricks CLI) |

**Recommendation:**
- Pure AWS → CodePipeline for ML + GitHub Actions for ETL
- Pure Databricks → Asset Bundles + GitHub Actions
- Hybrid → Both (they're independent)
