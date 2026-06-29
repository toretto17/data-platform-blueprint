# 🌍 How to: Deploy to a new AWS account or Databricks workspace

## AWS
1. Create `configs/<env>/project.yaml` with new account_id, region, bucket names, roles
2. Create `infrastructure/aws/terraform/env/<env>/etl-pipeline.tfvars`
3. Run `make tf-plan ENV=<env>` → review → `make tf-apply ENV=<env>`
4. Upload DDB configs: `ENV=<env> make load-ddb`
5. Upload Glue scripts: `make deploy-glue ENV=<env>`
6. Verify: trigger a test SF execution

## Databricks
1. Edit `infrastructure/databricks/asset-bundles/databricks.yml` — add a new target
2. Set workspace URL + authentication (token/SP)
3. Deploy: `databricks bundle deploy -t <env>`
4. Verify: trigger the workflow manually

## Pre-requisites (both)
- IAM roles / UC permissions must exist first
- S3 buckets / UC catalog+schema must be created (by TF or manually)
- Secrets (DB passwords, API keys) must be in Secrets Manager / secret scopes
- Network (VPC, firewall rules) must allow Glue/Spark to reach sources
