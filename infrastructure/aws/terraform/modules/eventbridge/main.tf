# EventBridge schedule → Step Function trigger
# CHANGE_ME: schedule_expression, state_machine_arn, role_arn

resource "aws_cloudwatch_event_rule" "schedule" {
  name                = var.rule_name
  schedule_expression = var.schedule_expression  # e.g. "cron(0 18 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "sf_target" {
  rule      = aws_cloudwatch_event_rule.schedule.name
  target_id = "sf-target"
  arn       = var.state_machine_arn
  role_arn  = var.event_role_arn
  input     = var.input_json
}

variable "rule_name" { type = string }
variable "schedule_expression" { type = string }
variable "state_machine_arn" { type = string }
variable "event_role_arn" { type = string }
variable "input_json" { type = string; default = "{}" }
