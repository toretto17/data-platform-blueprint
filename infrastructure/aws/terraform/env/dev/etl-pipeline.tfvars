# ============================================================
# Dev Environment — Terraform Variables
# ============================================================
# Copy this file and fill in your values.
# ============================================================

environment         = "dev"
project             = "CHANGE_ME"           # e.g., "bnic"
feature             = "CHANGE_ME"           # e.g., "aii"
account_id          = "CHANGE_ME"           # e.g., "503561443692"
region              = "ap-southeast-1"
assume_role_arn     = "arn:aws:iam::CHANGE_ME:role/CHANGE_ME-terraform-role"
glue_role_arn       = "arn:aws:iam::CHANGE_ME:role/GlueServiceRole-dev-CHANGE_ME"
sfn_execution_role_arn = "arn:aws:iam::CHANGE_ME:role/sfn-CHANGE_ME-dev-execution-role"
tag_project         = "CHANGE_ME Data Platform"
