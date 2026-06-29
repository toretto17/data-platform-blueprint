# 🚀 How to: Set Up CI/CD Pipeline (Step by Step)

Complete guide from zero to a working automated pipeline on AWS CodePipeline / GitHub Actions.

---

## What CI/CD Does For You

| Without CI/CD (today) | With CI/CD (automated) |
|---|---|
| Edit script → `aws s3 cp` → hope nothing broke | Push code → tests auto-run → auto-deploys if pass |
| No audit trail | Every deploy tracked (who, when, what) |
| Different people deploy differently | Same process every time |
| Rollback = manual panic | One-click rollback |
| Bugs found in production | Bugs caught before deployment |

---

## Architecture

```
CodeCommit/GitHub (source)
    → CodeBuild / GitHub Actions (test)
        → If tests PASS:
            → Upload scripts to S3 (Glue)
            → Terraform apply (infra changes)
            → Load DDB configs
            → Trigger Step Function (optional)
        → If tests FAIL:
            → ❌ Deployment BLOCKED
            → Alert sent (SNS/Slack)
```

---

## Option A: AWS CodePipeline + CodeBuild

### Prerequisites
- AWS account with CodeCommit/CodeBuild/CodePipeline access
- IAM role for CodeBuild (S3 + Glue + Terraform + DDB)
- S3 bucket for artifacts

### Step 1: Create a CodeCommit repo (or use GitHub)

```bash
# Create repo
aws codecommit create-repository --repository-name my-data-platform --region ap-southeast-1

# Clone + push your code
git remote add codecommit https://git-codecommit.ap-southeast-1.amazonaws.com/v1/repos/my-data-platform
git push codecommit main
```

### Step 2: Create the buildspec.yml

Already in your repo at `cicd/codebuild/buildspec.yaml`. Key phases:

```yaml
phases:
  install:     # Install Python + test deps
  pre_build:   # Run pytest — if FAILS, build stops here (safety net)
  build:       # Upload scripts to S3 + terraform apply (only if tests passed)
  post_build:  # Log results (runs even on failure)
```

### Step 3: Create a CodeBuild Project (Console)

1. Go to: **AWS Console → CodeBuild → Create build project**
2. Settings:
   - Name: `data-platform-build`
   - Source: CodeCommit (or GitHub) → select your repo → branch `main`
   - Environment:
     - Managed image → Ubuntu → Standard → Latest
     - Runtime: Python 3.11
     - Service role: create new (or use existing with S3/Glue/DDB permissions)
   - Buildspec: "Use a buildspec file" → path: `cicd/codebuild/buildspec.yaml`
3. Click **Create build project**
4. Test: click **Start build** → watch logs in real-time

### Step 4: Create a CodePipeline

1. Go to: **AWS Console → CodePipeline → Create pipeline**
2. Pipeline settings:
   - Name: `data-platform-pipeline`
   - New service role (or existing)
3. Source stage:
   - Provider: CodeCommit (or GitHub)
   - Repository: your repo
   - Branch: `main`
   - Detection: CloudWatch Events (triggers on push)
4. Build stage:
   - Provider: AWS CodeBuild
   - Project: `data-platform-build` (from Step 3)
5. Deploy stage:
   - Skip (our buildspec handles deployment in the build phase)
   - OR add a manual approval step before prod deploy
6. Click **Create pipeline**

### Step 5: Add manual approval for production

Go to pipeline → Edit → Add stage after Build:
- Stage name: `ApproveProduction`
- Action: Manual approval
- SNS topic: your alerts topic (notifies approvers)

Now: push to `main` → tests run → wait for approval → deploy to prod.

### Step 6: Set up notifications

```bash
# Create SNS topic for pipeline notifications
aws sns create-topic --name data-platform-pipeline-alerts
aws sns subscribe --topic-arn arn:aws:sns:CHANGE_ME:CHANGE_ME:data-platform-pipeline-alerts \
  --protocol email --notification-endpoint your@email.com

# Enable pipeline notifications
aws codestar-notifications create-notification-rule \
  --name "Pipeline Status" \
  --resource "arn:aws:codepipeline:CHANGE_ME:CHANGE_ME:data-platform-pipeline" \
  --detail-type FULL \
  --event-type-ids codepipeline-pipeline-pipeline-execution-failed codepipeline-pipeline-pipeline-execution-succeeded \
  --targets "[{\"TargetType\":\"SNS\",\"TargetAddress\":\"arn:aws:sns:CHANGE_ME:CHANGE_ME:data-platform-pipeline-alerts\"}]"
```

---

## Option B: GitHub Actions

Already configured in `cicd/github-actions/`:
- `ci.yaml` — runs on every PR (lint + test + terraform validate)
- `deploy.yaml` — runs on push to dev/main (upload scripts + terraform + DDB + Databricks)

### Setup

1. Go to: your GitHub repo → Settings → Environments
2. Create environments: `dev` and `prod`
3. For `prod`: enable **Required reviewers** (manual approval)
4. Go to: Settings → Secrets and variables → Actions
5. Add these secrets:

| Secret | Value |
|---|---|
| `AWS_DEPLOY_ROLE_DEV` | `arn:aws:iam::<dev-account>:role/github-actions-deploy-role` |
| `AWS_DEPLOY_ROLE_PROD` | `arn:aws:iam::<prod-account>:role/github-actions-deploy-role` |
| `AWS_ACCOUNT_ID_DEV` | `123456789012` |
| `AWS_ACCOUNT_ID_PROD` | `127214173492` |
| `DATABRICKS_HOST` | `your-workspace.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | `dapi...` |

6. Set up GitHub OIDC for AWS (no long-lived keys):
   - AWS Console → IAM → Identity providers → Add provider → OpenID Connect
   - URL: `https://token.actions.githubusercontent.com`
   - Audience: `sts.amazonaws.com`
   - Create a role that trusts this provider (attach S3/Glue/DDB policies)

7. Push to `dev` → pipeline runs automatically.

---

## Testing Strategy

### What to test (in your pytest suite)

| Test type | What it validates | Example |
|---|---|---|
| Unit tests | Individual functions work correctly | DQ checks pass/fail correctly |
| Schema tests | Output matches expected columns/types | Gold table has required columns |
| Business logic | Transformations produce correct values | SUM(daily_ga) matches expected |
| Edge cases | Handles nulls, empty data, duplicates | Empty DataFrame doesn't crash |

### Where tests live

```
tests/
  aws/unit/          ← test AWS-specific code
  databricks/unit/   ← test Databricks-specific code
  shared/            ← platform-neutral tests
  conftest.py        ← shared Spark fixtures
```

### Run locally before pushing

```bash
make lint       # ruff check
make test-unit  # pytest
```

---

## Rollback

### Automatic (S3 versioning)

If a bad script is deployed, S3 versioning lets you revert:

```bash
# List versions of a script
aws s3api list-object-versions --bucket CHANGE_ME --prefix scripts/silver_job.py

# Restore previous version
aws s3api copy-object \
  --bucket CHANGE_ME \
  --copy-source "CHANGE_ME/scripts/silver_job.py?versionId=<PREVIOUS_VERSION_ID>" \
  --key scripts/silver_job.py
```

### Manual (git revert)

```bash
git revert HEAD      # creates a new commit that undoes the last one
git push             # triggers pipeline → deploys the reverted code
```

---

## Cost

| Service | Cost | Notes |
|---|---|---|
| CodeCommit | Free (5 users) | First 5 active users free |
| CodeBuild | ~$0.005/min | First 100 min/month free |
| CodePipeline | $1/month per pipeline | Only active pipelines |
| GitHub Actions | Free (2000 min/month) | Public repos unlimited |
| S3 versioning | Negligible | Adds ~10% storage cost |

**Total for a basic pipeline: ~$2-5/month**

---

## Troubleshooting

| Issue | Cause | Fix |
|---|---|---|
| Tests pass locally but fail in CodeBuild | Different Python version or missing env vars | Set `runtime-versions` in buildspec + `PYTHONPATH` |
| "ModuleNotFoundError" in CodeBuild | PYTHONPATH not set | Add `export PYTHONPATH="${CODEBUILD_SRC_DIR}"` |
| Pipeline stuck on "InProgress" | CodeBuild timeout | Increase timeout in project settings (default 60min) |
| "Access Denied" on S3 upload | CodeBuild role missing S3 permissions | Attach `AmazonS3FullAccess` (or scoped policy) to role |
| GitHub Actions can't access AWS | OIDC not configured or wrong role ARN | Verify IAM identity provider + trust policy |
