variable "name" { type = string }
variable "role_arn" { type = string }
variable "definition" { type = string }

resource "aws_sfn_state_machine" "this" {
  name     = var.name
  role_arn = var.role_arn
  definition = var.definition
}

output "arn" { value = aws_sfn_state_machine.this.arn }
output "name" { value = aws_sfn_state_machine.this.name }
