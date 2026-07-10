# CI/CD

## Overview

This directory contains CI/CD configurations for two deployment tracks:

1. **ETL/Data Engineering** — Upload Glue scripts, load DDB configs, apply Terraform (GitHub Actions)
2. **ML/DS Model Lifecycle** — Build SageMaker Pipelines, promote models, deploy inference (CodePipeline + CodeBuild)

---

## Directory Structure

```
cicd/
├── github-actions/
│   ├── ci.yaml                      # Lint + test on every PR
│   └── deploy.yaml                  # Full ETL deploy (dev + prod + Databricks)
│
├── codebuild/
│   └── buildspec.yaml               # AWS CodeBuild for ETL (lint + test + deploy scripts)
│
├── codepipeline/
│   ├── README.md                    # Full setup guide for ML CI/CD
│   ├── ml_build_buildspec.yaml      # Upserts SageMaker Training + Inference pipelines
│   ├── ml_deploy_buildspec.yaml     # Promotes models + deploys inference + monitoring
│   ├── build.py                     # Model promotion script (cross-account)
│   └── sync_repos.sh               # Syncs main repo → CodeCommit build/deploy repos
│
└── deployment/
    └── deploy_glue_scripts.sh       # Upload all ETL scripts to S3 artifactory
```

---

## ETL CI/CD (GitHub Actions)

**Flow:**
```
PR → ci.yaml (lint + test + terraform validate)
     → merge to dev → deploy.yaml (upload scripts + DDB load + terraform apply to dev)
     → merge to main → deploy.yaml (same to prod, requires manual approval)
```

| File | Trigger | What it does |
|------|---------|-------------|
| `ci.yaml` | Every PR | ruff lint, pytest, terraform validate |
| `deploy.yaml` | Push to dev/main | Upload ETL scripts → Load DDB configs → Terraform apply |

---

## ML CI/CD (CodePipeline + CodeBuild)

**Flow:**
```
Developer → sync_repos.sh → CodeCommit BUILD repo → CodePipeline →
  CodeBuild (BUILD): Upserts SageMaker Pipelines (train + inference) in nonprod
       ↓
  (Model trained → Approved in SageMaker Studio)
       ↓
  CodeCommit DEPLOY repo → CodePipeline →
  Manual Approval Gate →
  CodeBuild (DEPLOY): Promotes model → deploys inference → triggers monitoring (in prod)
```

| File | Purpose | Called by |
|------|---------|-----------|
| `ml_build_buildspec.yaml` | Upserts SageMaker Pipelines | CodeBuild on push to BUILD repo |
| `ml_deploy_buildspec.yaml` | Promotes + deploys + monitors | CodeBuild after manual approval |
| `build.py` | Model promotion logic (per model group) | Called by deploy buildspec in a loop |
| `sync_repos.sh` | Syncs mlops/ code → CodeCommit repos | Run manually or on merge |

**Full documentation:** See [`codepipeline/README.md`](codepipeline/README.md)

---

## Quick Start

### For ETL changes:
```bash
# Push code → GitHub Actions handles lint/test/deploy
git push origin dev
```

### For ML model changes:
```bash
# 1. Edit mlops/ code
# 2. Sync to CodeCommit
./cicd/codepipeline/sync_repos.sh

# 3. BUILD pipeline auto-triggers (upserts SageMaker Pipelines)
# 4. After model approval, DEPLOY pipeline promotes to prod
```

### For Glue script deployment only:
```bash
./cicd/deployment/deploy_glue_scripts.sh dev us-east-1
```

---

## Choosing Between GitHub Actions and CodePipeline

| Criterion | GitHub Actions | CodePipeline |
|-----------|---------------|--------------|
| Best for | ETL scripts, Terraform, DDB configs | SageMaker model lifecycle |
| Trigger | Git push/PR to GitHub | Git push to CodeCommit |
| Cross-account | Via OIDC role assumption | Via sts:AssumeRole in buildspec |
| Manual approval | GitHub environment protection | CodePipeline approval stage |
| Cost | Free (public repo) / included minutes | Pay per execution |
| Integration | General purpose (any cloud) | Deep AWS-native (SageMaker, S3, IAM) |

**Recommendation:** Use both. GitHub Actions for ETL (simple, fast). CodePipeline for ML (SageMaker-native, cross-account IAM, model registry integration).
