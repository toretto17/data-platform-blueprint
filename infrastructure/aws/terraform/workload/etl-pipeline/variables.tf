variable "region" {
  type    = string
  default = "ap-southeast-1"
}

variable "environment" {
  type        = string
  description = "Environment name (dev/qa/uat/prod)"
}

variable "project" {
  type        = string
  description = "Project identifier (e.g., bnic, myco)"
}

variable "feature" {
  type        = string
  description = "Feature/domain identifier (e.g., aii, revenue)"
}

variable "account_id" {
  type        = string
  description = "AWS Account ID"
}

variable "assume_role_arn" {
  type        = string
  description = "IAM role ARN for Terraform to assume"
}

variable "glue_role_arn" {
  type        = string
  description = "IAM role ARN for Glue jobs"
}

variable "sfn_execution_role_arn" {
  type        = string
  description = "IAM role ARN for Step Functions execution"
}

variable "tag_project" {
  type        = string
  description = "Tag value for Project"
}
