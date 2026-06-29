# infrastructure/

| Path | Purpose |
|---|---|
| `aws/terraform/modules/glue/` | Glue job Terraform module |
| `aws/terraform/modules/sfn/` | Step Function module |
| `aws/terraform/modules/s3/` | S3 bucket (versioned, encrypted, no public access) |
| `aws/terraform/modules/iam/` | IAM roles (Glue + SageMaker + SF) |
| `aws/terraform/modules/eventbridge/` | EventBridge schedule → SF trigger |
| `aws/terraform/workload/etl-pipeline/` | Full ETL pipeline stack (Glue jobs + SFs) |
| `aws/stepfunctions/examples/` | Real production SF JSON examples |
| `aws/lambda/config-loader/` | Lambda to load DDB configs from S3 |
| `databricks/asset-bundles/databricks.yml` | Databricks Asset Bundle (deploy jobs/clusters) |

## Deploy
```bash
# AWS
make tf-plan ENV=dev     # review
make tf-apply ENV=dev    # apply

# Databricks
cd infrastructure/databricks/asset-bundles
databricks bundle deploy -t dev
```
