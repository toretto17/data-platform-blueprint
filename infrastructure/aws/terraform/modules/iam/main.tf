# IAM roles module — Glue + SageMaker + SF execution roles
# CHANGE_ME: account_id, environment, project

resource "aws_iam_role" "glue_role" {
  name               = "GlueServiceRole-${var.environment}-${var.project}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow",
                   Principal = { Service = "glue.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role" "sagemaker_role" {
  name               = "iam-${var.project}-mlops-${var.environment}-sagemaker-default-execution-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow",
                   Principal = { Service = "sagemaker.amazonaws.com" } }]
  })
}

variable "environment" { type = string }
variable "project" { type = string }
output "glue_role_arn" { value = aws_iam_role.glue_role.arn }
output "sagemaker_role_arn" { value = aws_iam_role.sagemaker_role.arn }
