# CI/CD

| File | Purpose |
|---|---|
| `github-actions/ci.yaml` | Lint + unit test on every PR |
| `github-actions/deploy.yaml` | Deploy to dev/prod (existed from original build) |
| `codebuild/buildspec.yaml` | AWS CodeBuild equivalent (lint + test + deploy scripts) |
| `deployment/deploy_glue_scripts.sh` | Upload all Glue .py files to S3 artifactory |

## Flow
```
PR → ci.yaml (lint+test) → merge → deploy.yaml → upload scripts + terraform apply
```
