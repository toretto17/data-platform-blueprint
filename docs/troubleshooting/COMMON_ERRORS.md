# Common Errors & Fixes

## Data Engineering

| Error | Cause | Fix |
|---|---|---|
| `AnalysisException: TABLE_OR_VIEW_NOT_FOUND` | Table not registered in catalog | Run `MSCK REPAIR TABLE` (AWS) or check UC permissions (Databricks) |
| `UnboundLocalError` in a loop | Variable only assigned inside try/except that failed | Initialize variable before the loop |
| 3 months instead of ALL months | MODE=append + LOOKBACK_DAYS=60 (DDB overrides Terraform) | Set MODE=overwrite for backfill |
| Wrong column for different products | COALESCE chain picks wrong value (0 vs NULL) | Use NULLIF(col, 0) before COALESCE |
| Duplicate rows after JOIN | Missing dimension in JOIN key | Add ALL PK dimensions to the ON clause |
| Rate column > 100% | SUM of daily rates (wrong — should be ratio-of-sums) | Recompute as SUM(numerator) / SUM(denominator) |

## MLOps

| Error | Cause | Fix |
|---|---|---|
| `ResourceNotFound: Feature Group` | FG not created yet | Run creation/feature_group.py first |
| `AccessDenied` on S3 | IAM role missing bucket policy | Add s3:GetObject/PutObject to the Glue/SageMaker role |
| Model Monitor not triggering | Monitoring DDB config missing or wrong group name | Check `mlops_model_monitoring_config` DDB |
| `EndpointInService` but wrong predictions | Old model version still serving | Check endpoint config → model → package ARN |

## Platform

| Error | Cause | Fix |
|---|---|---|
| `Terraform: 403` | Assume-role expired or wrong profile | Refresh creds or check `assume_role_arn` in tfvars |
| `databricks bundle: no workspace` | Missing `databricks.yml` target config | Set target in `databricks.yml` or pass `-t dev` |
| `spark.sql.catalog not found` | Delta/Iceberg extension not loaded | Add `--conf spark.sql.extensions=...` to Glue params |
