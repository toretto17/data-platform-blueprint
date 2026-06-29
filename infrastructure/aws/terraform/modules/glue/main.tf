variable "job_name" { type = string }
variable "role_arn" { type = string }
variable "script_location" { type = string }
variable "glue_version" { type = string; default = "4.0" }
variable "worker_type" { type = string; default = "G.1X" }
variable "number_of_workers" { type = number; default = 2 }
variable "timeout" { type = number; default = 60 }
variable "max_retries" { type = number; default = 0 }
variable "max_concurrent_runs" { type = number; default = 1 }
variable "default_arguments" { type = map(string); default = {} }
variable "extra_jars" { type = string; default = null }

resource "aws_glue_job" "this" {
  name     = var.job_name
  role_arn = var.role_arn

  command {
    script_location = var.script_location
    python_version  = "3"
  }

  glue_version = var.glue_version
  worker_type  = var.worker_type
  number_of_workers = var.number_of_workers
  timeout      = var.timeout
  max_retries  = var.max_retries

  execution_property {
    max_concurrent_runs = var.max_concurrent_runs
  }

  default_arguments = merge(
    var.default_arguments,
    var.extra_jars != null ? { "--extra-jars" = var.extra_jars } : {}
  )
}

output "name" { value = aws_glue_job.this.name }
output "arn" { value = aws_glue_job.this.arn }
