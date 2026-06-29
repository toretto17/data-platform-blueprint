/*
 * Enterprise Data Platform — Terraform ETL Pipeline
 *
 * Provisions: Glue Jobs, Step Functions, EventBridge Rules, S3 artifacts
 * All resources parameterized via variables (env-specific tfvars files).
 *
 * Usage:
 *   cd infrastructure/terraform/env/dev
 *   terraform init -backend-config=backend.hcl
 *   terraform plan -var-file=etl-pipeline.tfvars
 *   terraform apply -var-file=etl-pipeline.tfvars
 */

provider "aws" {
  region = var.region
  assume_role {
    role_arn     = var.assume_role_arn
    session_name = "tf-workload-etl-creation"
  }
  default_tags {
    tags = {
      Environment = var.environment
      Project     = var.tag_project
      ManagedBy   = "Terraform"
    }
  }
}

# ============================================================
# LOCALS — Derived bucket names and common values
# ============================================================
locals {
  bucket_prefix      = "s3-${var.project}-${var.feature}-${var.environment}"
  bucket_silver      = "${local.bucket_prefix}-silver-${var.account_id}"
  bucket_gold        = "${local.bucket_prefix}-gold-${var.account_id}"
  bucket_consumption = "${local.bucket_prefix}-consumption-${var.account_id}"
  bucket_dq          = "${local.bucket_prefix}-data-quality-${var.account_id}"
  bucket_artifactory = "${local.bucket_prefix}-artifactory-${var.account_id}"
  bucket_feature     = "${local.bucket_prefix}-feature-${var.account_id}"

  # Common template vars for Step Function rendering
  common_template_vars = {
    environment = var.environment
    account_id  = var.account_id
    region      = var.region
    project     = var.project
    feature     = var.feature
  }
}

# ============================================================
# GLUE JOBS — Created from locals-gluejobs.tf list
# ============================================================
module "glue_jobs" {
  source   = "../../modules/glue/job"
  for_each = { for job in local.gluejobs_list : job.name => job }

  job_name           = each.value.name
  role_arn           = var.glue_role_arn
  glue_version       = lookup(each.value, "glue_version", "4.0")
  worker_type        = lookup(each.value, "worker_type", "G.1X")
  number_of_workers  = lookup(each.value, "number_of_workers", 2)
  timeout            = lookup(each.value, "timeout", 60)
  max_retries        = lookup(each.value, "max_retries", 0)
  max_concurrent_runs = lookup(each.value, "max_concurrent_runs", 1)

  script_location    = "s3://${local.bucket_artifactory}/scripts/${lookup(each.value, "source_file", each.value.name)}.py"
  extra_jars         = lookup(each.value, "extra_jars", null) != null ? "s3://${local.bucket_artifactory}/${each.value.extra_jars}" : null

  default_arguments = merge(
    {
      "--enable-metrics"                = "true"
      "--enable-continuous-cloudwatch-log" = "true"
      "--enable-spark-ui"               = "true"
      "--job-language"                   = "python"
      "--TempDir"                        = "s3://${local.bucket_artifactory}/glue-temp/"
    },
    lookup(each.value, "additional_arguments", {})
  )
}

# ============================================================
# STEP FUNCTIONS — Created from locals-sfn.tf list
# ============================================================
module "step_functions" {
  source   = "../../modules/sfn"
  for_each = { for sf in local.sfn_list : sf.name => sf }

  name       = "${var.environment}-${each.value.name}"
  role_arn   = var.sfn_execution_role_arn
  definition = templatefile(
    "${path.root}/../../../src/orchestration/stepfunctions/${each.value.config_file}",
    local.common_template_vars
  )
}

# ============================================================
# EVENTBRIDGE RULES — Scheduled triggers
# ============================================================
resource "aws_cloudwatch_event_rule" "scheduled_pipelines" {
  for_each = { for sched in local.schedules_list : sched.name => sched }

  name                = "${var.environment}-${each.value.name}-schedule"
  schedule_expression = each.value.cron
  state               = each.value.enabled ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "sfn_targets" {
  for_each = { for sched in local.schedules_list : sched.name => sched }

  rule      = aws_cloudwatch_event_rule.scheduled_pipelines[each.key].name
  target_id = "${each.value.name}-target"
  arn       = module.step_functions[each.value.sf_name].arn
  role_arn  = var.sfn_execution_role_arn
  input     = jsonencode({ dl_date = each.value.input_template })
}
