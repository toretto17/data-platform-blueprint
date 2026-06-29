# Infrastructure — Step Functions, Lambda & DDB Config

## Architecture Flow

```
EventBridge (cron)
    → Master Pipeline SF (domain-specific DAG: parallel Silver+Gold → Consumption)
        → Framework Transformation SF (generic: read DDB → run Glue → Redshift load)
            → Glue Job (actual ETL)
            → Redshift Dataload SF (COPY from S3 into Redshift)
```

## Step Functions

### 1. `framework_transformation_sf.json` (869 lines)
**The core orchestration engine.** Generic — works for ANY Glue job.

**What it does:**
1. Reads job config from DynamoDB (`job_name` → full config with args, step controls)
2. Runs Silver Glue job (if `enable_silver=true`)
3. Runs Gold Glue job (if `enable_gold=true`)
4. Runs Redshift load (if `enable_redshift=true`) by invoking `redshift_dataload_sf`
5. Logs success/failure to DDB + publishes SNS

**Input:**
```json
{"job_name": "silver_gold_mobile_sales_pipeline", "dl_date": "2026-06-01"}
```

**Key states:**
- `2a_get_job_config` → Lambda reads DDB config
- `4_3b_start_job_run` → Starts Glue job with args from DDB
- `6b_start_redshift_load` → Invokes Redshift SF with `redshift_config` from DDB
- `7a_log_fw_success_status` → Logs to DDB log table
- `8a_publish_success_msg_to_sns` → SNS notification

### 2. `redshift_dataload_sf.json` (136 lines)
**Loads data from S3 into Redshift** using COPY command via Lambda.

**Input (from framework SF):**
```json
{
  "target_tables_list": [
    {
      "target_database": "my_db",
      "target_schema": "consumption_layer",
      "target_table": "sales_mart",
      "s3_path": "s3://bucket/consumption/sales_mart/"
    }
  ],
  "dl_date": "2026-06-01"
}
```

**What it does:**
1. Iterates over `target_tables_list`
2. For each table: runs COPY from S3 path into Redshift table
3. Handles errors per table (one failure doesn't block others)

### 3. `master_pipeline_sf.json` (201 lines)
**Domain-specific orchestration** (example: Sales pipeline).

**What it does:**
1. Formats `dl_date` from EventBridge input
2. Runs parallel branches (Silver+Gold, Cellsite mapping)
3. After parallel completes → runs Consumption
4. Supports `mode=history` for backfill (different DDB job_names)

**Input (from EventBridge):**
```json
{"dl_date": "2026-06-25T03:30:00Z"}
```

## Lambda

### `config-loader/lambda_function.py` (327 lines)
**Loads DDB config JSONs from S3 into DynamoDB.**

**How it works:**
1. Reads JSON files from `s3://{bucket}/{prefix}/`
2. Validates DDB format (S/N/M/L/BOOL types)
3. Batch writes to DynamoDB table

**Environment variables:**
- `S3_BUCKET`: Artifactory bucket (e.g., `s3-project-feature-env-artifactory-accountid`)
- `S3_PREFIX`: `config/job`
- `DYNAMODB_TABLE`: `project-feature-fw-config-table`

**Triggered by:** `load_ddb_config.sh` (see `configs/scripts/`)

## DDB Configuration Examples

### `example_consumption_with_redshift_ddb_config.json`
Full example showing:
- `job_name` — matched by framework SF
- `glue_transformation_gold_config` — Glue job name + arguments
- `step_control` — which steps to run (silver/gold/redshift)
- `redshift_config` — target tables for Redshift COPY

**Redshift config structure:**
```json
{
  "redshift_config": {
    "M": {
      "target_tables_list": {
        "L": [
          {
            "M": {
              "target_database": {"S": "my_warehouse_db"},
              "target_schema": {"S": "consumption_layer"},
              "target_table": {"S": "sales_mart"},
              "s3_path": {"S": "s3://bucket-consumption-accountid/sales_mart/"}
            }
          }
        ]
      }
    }
  }
}
```

### `example_silver_gold_pipeline_ddb_config.json`
Shows Silver + Gold config with:
- `--LOOKBACK_DAYS`, `--MODE`, `--TARGET_BUCKET`, `--TARGET_DATABASE`, `--TARGET_TABLE`
- `--DQ_BUCKET`, `--PARTITION_COLUMN`
- Source configs array

## Deployment Flow

```bash
# 1. Edit DDB config JSONs (use ${environment} and ${account_id} placeholders)
vi configs/dev/dynamodb/my_pipeline_ddb_config.json

# 2. Upload to S3 + trigger Lambda (renders placeholders, loads to DDB)
ENV=dev ./configs/scripts/load_ddb_config.sh

# 3. Deploy Step Functions via Terraform
cd infrastructure/terraform/env/dev
terraform apply -var-file=etl-pipeline.tfvars

# 4. Trigger manually or wait for EventBridge schedule
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:REGION:ACCOUNT:stateMachine:ENV-PROJECT-DOMAIN-master-pipeline" \
  --input '{"dl_date": "2026-06-25T00:00:00Z"}'
```
