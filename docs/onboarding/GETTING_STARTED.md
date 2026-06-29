# Getting Started — Enterprise Data Platform

## Prerequisites

- AWS CLI configured with appropriate credentials
- Python 3.11+
- Terraform 1.5+
- Docker (for BYOC ML images)

## Step 1: Clone & Configure

```bash
git clone <template-repo> my-project
cd my-project

# Copy template configs
cp configs/templates/project.yaml.template configs/dev/project.yaml
```

Edit `configs/dev/project.yaml`:
```yaml
project: "mycompany"
feature: "analytics"
environment: "dev"
account_id: "123456789012"
region: "us-east-1"
```

## Step 2: Update Python Config

Edit `src/common/constants/config.py`:
- Set `PROJECT`, `FEATURE`, `DOMAIN`, `ACCOUNT_ID` in `DevConfig` and `ProdConfig`

## Step 3: Deploy Infrastructure

```bash
cd infrastructure/terraform/env/dev

# Create backend.hcl with your S3 state bucket
cat > backend.hcl << EOF
bucket         = "my-terraform-state-bucket"
key            = "data-platform/dev/terraform.tfstate"
region         = "us-east-1"
dynamodb_table = "terraform-locks"
EOF

# Create tfvars
cat > etl-pipeline.tfvars << EOF
environment         = "dev"
project             = "mycompany"
feature             = "analytics"
account_id          = "123456789012"
region              = "us-east-1"
assume_role_arn     = "arn:aws:iam::123456789012:role/TerraformRole"
glue_role_arn       = "arn:aws:iam::123456789012:role/GlueServiceRole"
sfn_execution_role_arn = "arn:aws:iam::123456789012:role/StepFunctionsRole"
tag_project         = "MyDataPlatform"
EOF

terraform init -backend-config=backend.hcl
terraform plan -var-file=etl-pipeline.tfvars
terraform apply -var-file=etl-pipeline.tfvars
```

## Step 4: Create Your First ETL Job

1. Copy template:
```bash
cp src/silver/jobs/silver_job_template.py src/silver/jobs/glue_myco_analytics_silver_sales.py
```

2. Implement your logic (override `_define_sources`, `_apply_transformations`)

3. Create DDB config:
```bash
cp configs/templates/ddb_config.json.template configs/dev/dynamodb/silver_gold_sales_pipeline.json
# Edit with your table names, buckets, etc.
```

4. Upload & deploy:
```bash
# Upload script
aws s3 cp src/silver/jobs/glue_myco_analytics_silver_sales.py \
  s3://s3-myco-analytics-dev-artifactory-123456789012/scripts/

# Load DDB config
ENV=dev ./configs/scripts/load_ddb_config.sh silver_gold_sales_pipeline.json
```

5. Trigger:
```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:123456789012:stateMachine:sfn-myco-analytics-fw-transformation" \
  --input '{"job_name": "silver_gold_sales_pipeline", "dl_date": "2026-01-01"}'
```

## Step 5: Add More Layers

Follow the same pattern for Gold and Consumption:
- `src/gold/marts/` — Business aggregations
- `src/consumption/athena/` — Reporting-ready tables

## Directory Conventions

| I want to... | Go to... |
|---|---|
| Add a Silver ETL job | `src/silver/jobs/` |
| Add a Gold aggregation | `src/gold/marts/` |
| Add DQ checks | `src/common/validations/` |
| Add a Feature Store job | `src/feature_store/ingestion/` |
| Add an ML pipeline | `src/mlops/training/` |
| Add infrastructure | `infrastructure/terraform/workload/` |
| Add a schedule | `infrastructure/terraform/workload/etl-pipeline/locals.tf` |
| Add a Step Function | `infrastructure/stepfunctions/` |
| Add monitoring | `monitoring/cloudwatch/` |

## Naming Convention Cheat Sheet

```
Glue Job:    glue_{project}_{feature}_{layer}_{domain}
S3 Bucket:   s3-{project}-{feature}-{env}-{layer}-{account_id}
SF:          {env}-{project}-{domain}-master-pipeline
DDB Job:     {domain}_{layer}_pipeline
Database:    {domain}_analytics_{layer}
Table:       {layer}_{domain}  (silver_sales, gold_sales_mart)
```
