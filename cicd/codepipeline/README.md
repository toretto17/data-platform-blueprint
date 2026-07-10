# ML CI/CD — CodePipeline + CodeBuild for SageMaker

## Overview

This directory contains production-grade CI/CD templates for ML model lifecycle management on AWS. The system handles:

1. **Building** — Upserts SageMaker Training + Inference pipelines in nonprod
2. **Deploying** — Promotes approved models to prod, deploys inference, triggers monitoring
3. **Syncing** — Keeps CodeCommit CI/CD repos in sync with main development repo

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        TOOLING ACCOUNT (111111111111)                     │
│                                                                         │
│  CodeCommit (BUILD repo)  ──→  CodePipeline  ──→  CodeBuild            │
│       push triggers               │                  │                  │
│                                   │           sts:AssumeRole            │
│                                   ▼                  │                  │
│  CodeCommit (DEPLOY repo) ──→  CodePipeline          │                  │
│       push + manual approval      │                  │                  │
└───────────────────────────────────┼──────────────────┼──────────────────┘
                                    │                  │
                    ┌───────────────┼──────────────────┼───────────────┐
                    │               ▼                  ▼               │
                    │   NONPROD ACCOUNT (222222222222)                    │
                    │                                                  │
                    │   SageMaker Pipelines (Training + Inference)     │
                    │   Model Registry (ModelPackageGroups)            │
                    │   Feature Store                                  │
                    │   S3 (model artifacts)                           │
                    └─────────────────────────────────────────────────┘
                                    │
                         model promotion (cross-account copy)
                                    │
                    ┌───────────────┼──────────────────────────────────┐
                    │               ▼                                  │
                    │   PROD ACCOUNT (333333333333)                       │
                    │                                                  │
                    │   Model Registry (promoted packages)             │
                    │   SageMaker Pipelines (Inference only)           │
                    │   Monitoring (DQ baselines, drift detection)     │
                    │   Step Functions (daily inference orchestration)  │
                    └─────────────────────────────────────────────────┘
```

---

## Files

| File | Purpose | When to Use |
|------|---------|-------------|
| `ml_build_buildspec.yaml` | Upserts SageMaker Pipelines in nonprod | Every code push to BUILD repo |
| `ml_deploy_buildspec.yaml` | Promotes models + deploys inference + triggers monitoring | After manual approval in CodePipeline |
| `build.py` | Python script for model promotion (called by deploy buildspec) | Part of deploy buildspec flow |
| `sync_repos.sh` | Syncs main repo → CodeCommit BUILD/DEPLOY repos | Run manually or on merge to main |
| `README.md` | This documentation | Reference |

---

## Setup Guide

### Step 1: Create CodeCommit Repositories

For each model family, create 2 repos in CodeCommit:

```bash
# BUILD repo (triggers pipeline upsert)
aws codecommit create-repository \
    --repository-name "CHANGE_ME-model-a-build" \
    --repository-description "ML build: upserts SageMaker pipelines"

# DEPLOY repo (triggers model promotion)
aws codecommit create-repository \
    --repository-name "CHANGE_ME-model-a-deploy" \
    --repository-description "ML deploy: promotes models to prod"
```

### Step 2: Create IAM Roles

#### In Tooling Account (where CodeBuild runs):

```json
{
    "Effect": "Allow",
    "Action": "sts:AssumeRole",
    "Resource": [
        "arn:aws:iam::NONPROD_ACCOUNT_ID:role/iam-CHANGE_ME-nonprod-codebuild-service-role",
        "arn:aws:iam::PROD_ACCOUNT_ID:role/iam-CHANGE_ME-prod-codebuild-service-role"
    ]
}
```

#### In Nonprod/Prod Accounts (CodeBuild assumes into these):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sagemaker:*Pipeline*",
                "sagemaker:*ModelPackage*",
                "sagemaker:*Model",
                "sagemaker:ListTags",
                "sagemaker:AddTags",
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket",
                "states:StartExecution",
                "iam:PassRole"
            ],
            "Resource": "*"
        }
    ]
}
```

Trust policy for cross-account assumption:
```json
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {
            "AWS": "arn:aws:iam::TOOLING_ACCOUNT_ID:root"
        },
        "Action": "sts:AssumeRole"
    }]
}
```

### Step 3: Create CodeBuild Projects

#### BUILD Project:

```bash
aws codebuild create-project \
    --name "CHANGE_ME-ml-build" \
    --source '{"type":"CODECOMMIT","location":"CHANGE_ME-model-a-build"}' \
    --artifacts '{"type":"NO_ARTIFACTS"}' \
    --environment '{
        "type": "LINUX_CONTAINER",
        "image": "aws/codebuild/standard:7.0",
        "computeType": "BUILD_GENERAL1_SMALL",
        "environmentVariables": [
            {"name":"NONPROD_ACCOUNT_ID","value":"CHANGE_ME"},
            {"name":"NONPROD_SAGEMAKER_ROLE_ARN","value":"arn:aws:iam::CHANGE_ME:role/SageMakerRole"}
        ]
    }' \
    --service-role "arn:aws:iam::TOOLING_ACCOUNT:role/CodeBuildRole"
```

#### DEPLOY Project:

```bash
aws codebuild create-project \
    --name "CHANGE_ME-ml-deploy" \
    --source '{"type":"CODECOMMIT","location":"CHANGE_ME-model-a-deploy"}' \
    --artifacts '{"type":"S3","location":"CHANGE_ME-artifacts-bucket"}' \
    --environment '{
        "type": "LINUX_CONTAINER",
        "image": "aws/codebuild/standard:7.0",
        "computeType": "BUILD_GENERAL1_SMALL",
        "environmentVariables": [
            {"name":"DEPLOY_STAGE","value":"nonprod"},
            {"name":"NONPROD_ACCOUNT_ID","value":"CHANGE_ME"},
            {"name":"PROD_ACCOUNT_ID","value":"CHANGE_ME"},
            {"name":"NONPROD_SAGEMAKER_ROLE_ARN","value":"CHANGE_ME"},
            {"name":"PROD_SAGEMAKER_ROLE_ARN","value":"CHANGE_ME"},
            {"name":"SAGEMAKER_PROJECT_NAME","value":"CHANGE_ME"},
            {"name":"SAGEMAKER_PROJECT_ID","value":"CHANGE_ME"},
            {"name":"SAGEMAKER_PROJECT_ARN","value":"CHANGE_ME"},
            {"name":"AWS_REGION","value":"us-east-1"}
        ]
    }' \
    --service-role "arn:aws:iam::TOOLING_ACCOUNT:role/CodeBuildRole"
```

### Step 4: Create CodePipeline

```json
{
    "pipeline": {
        "name": "CHANGE_ME-ml-deploy-pipeline",
        "stages": [
            {
                "name": "Source",
                "actions": [{
                    "name": "Source",
                    "actionTypeId": {"category":"Source","owner":"AWS","provider":"CodeCommit","version":"1"},
                    "configuration": {
                        "RepositoryName": "CHANGE_ME-model-a-deploy",
                        "BranchName": "main"
                    },
                    "outputArtifacts": [{"name":"SourceArtifact"}]
                }]
            },
            {
                "name": "Approval",
                "actions": [{
                    "name": "ManualApproval",
                    "actionTypeId": {"category":"Approval","owner":"AWS","provider":"Manual","version":"1"},
                    "configuration": {
                        "CustomData": "Approve model promotion to prod?"
                    }
                }]
            },
            {
                "name": "Deploy",
                "actions": [{
                    "name": "DeployToProd",
                    "actionTypeId": {"category":"Build","owner":"AWS","provider":"CodeBuild","version":"1"},
                    "configuration": {
                        "ProjectName": "CHANGE_ME-ml-deploy",
                        "EnvironmentVariables": "[{\"name\":\"DEPLOY_STAGE\",\"value\":\"prod\",\"type\":\"PLAINTEXT\"}]"
                    },
                    "inputArtifacts": [{"name":"SourceArtifact"}]
                }]
            }
        ]
    }
}
```

### Step 5: Configure `cicd-requirements.txt`

```txt
boto3>=1.34.0
sagemaker>=2.200.0
```

### Step 6: Configure `sync_repos.sh`

Edit the variables at the top:
1. `MLOPS_DIR` → path to your `mlops/` directory
2. `MAIN_REPO_DIR` → path to your main repo root
3. `BUILD_REPOS` → your CodeCommit BUILD repo URLs
4. `DEPLOY_REPOS` → your CodeCommit DEPLOY repo URLs

Run:
```bash
# Export tooling account credentials first
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...

./cicd/codepipeline/sync_repos.sh
```

---

## Daily Workflow

### Developer Flow:

```
1. Edit ML code in main repo (mlops/)
2. Run tests locally: make test
3. Commit + push to main repo (git push origin dev)
4. Run sync: ./cicd/codepipeline/sync_repos.sh
5. CodePipeline triggers BUILD → pipelines upserted in nonprod
6. Validate: check SageMaker console for updated pipelines
7. When ready for prod: push to DEPLOY repo (sync does this)
8. Approve in CodePipeline console
9. DEPLOY runs → models promoted + inference deployed + monitoring triggered
```

### Automated Flow (ideal state):

```
1. Merge PR to main → GitHub Action syncs to CodeCommit (or use CodeCommit directly)
2. BUILD pipeline auto-triggers → pipelines upserted
3. Scheduled retrain runs → new model trained + registered
4. Data scientist approves model in SageMaker Studio
5. DEPLOY pipeline auto-triggers (watches for new Approved packages)
6. Manual approval gate in CodePipeline
7. Prod deployment executes
```

---

## Environment Variables Reference

| Variable | Where Set | Purpose |
|----------|-----------|---------|
| `DEPLOY_STAGE` | CodeBuild/CodePipeline | "nonprod" or "prod" — determines target account |
| `NONPROD_ACCOUNT_ID` | CodeBuild env vars | AWS account ID for nonprod |
| `PROD_ACCOUNT_ID` | CodeBuild env vars | AWS account ID for prod |
| `NONPROD_SAGEMAKER_ROLE_ARN` | CodeBuild env vars | SageMaker execution role ARN (nonprod) |
| `PROD_SAGEMAKER_ROLE_ARN` | CodeBuild env vars | SageMaker execution role ARN (prod) |
| `SAGEMAKER_PROJECT_NAME` | CodeBuild env vars | SageMaker Project name (for tagging) |
| `SAGEMAKER_PROJECT_ID` | CodeBuild env vars | SageMaker Project ID |
| `SAGEMAKER_PROJECT_ARN` | CodeBuild env vars | SageMaker Project ARN |
| `AWS_REGION` | CodeBuild env vars | AWS region (e.g., us-east-1, eu-west-1) |
| `MLOPS_SAGEMAKER_ROLE_NAME` | Derived in buildspec | Role name extracted from ARN (for pipeline builder) |

---

## Troubleshooting

### "No approved ModelPackage found"
- Model hasn't been approved yet in SageMaker Studio
- Check: `aws sagemaker list-model-packages --model-package-group-name "GROUP" --model-approval-status Approved`

### "Access Denied" on sts:AssumeRole
- Trust policy on target role doesn't include the tooling account
- Check: `aws iam get-role --role-name "target-role" --query 'Role.AssumeRolePolicyDocument'`

### "Pipeline upsert fails"
- PYTHONPATH not set correctly — ensure `$CODEBUILD_SRC_DIR` and `utils` are included
- Check: verify `pipelines/` module can be imported (`python -c "import pipelines"`)

### "S3 copy fails (cross-account)"
- Source bucket policy doesn't allow GetObject from target role
- Fix: add bucket policy OR use the download+upload fallback in build.py

### "Monitoring SF trigger fails"
- Step Function doesn't exist in target account yet
- Deploy monitoring infrastructure (Terraform) before running ML deploy

---

## Single-Account Simplification

If you don't have a multi-account setup (everything in one account):

1. **Remove** all `sts:AssumeRole` steps from buildspecs
2. **Remove** the `pre_build` phase entirely
3. **Set** `--training-id` and `--target-id` to the same account ID
4. `build.py` will skip cross-account copy (same-account path)
5. Remove `ASSUME_ROLE_ARN` environment variables

---

## Adding a New Model Family

1. Create 2 new CodeCommit repos (build + deploy)
2. Create a new `cicd/<model>_build/` folder with `buildspec.yaml` + `cicd-requirements.txt`
3. Create a new `cicd/<model>_deploy/` folder with `buildspec.yaml` + `build.py` + `_monitoring_defaults.py`
4. Add entries to `sync_repos.sh` (`BUILD_REPOS` and `DEPLOY_REPOS` arrays)
5. Create CodeBuild + CodePipeline for the new model
6. Update `pipelines/list_groups.py` to support `--model new_model_name`

---

## Security Considerations

- **Least privilege**: CodeBuild role only has `sts:AssumeRole` to specific target roles
- **No credentials in code**: All secrets via environment variables or IAM role assumption
- **Approval gate**: Manual approval required before prod deployment
- **Audit trail**: CodePipeline execution history + CloudTrail for all API calls
- **Artifact isolation**: Each account has its own S3 bucket for model artifacts
- **Image scanning**: ECR image scanning should be enabled on all inference container repos
