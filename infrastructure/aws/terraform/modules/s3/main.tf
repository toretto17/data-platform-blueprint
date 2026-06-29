# S3 Bucket module — data lake layers
# CHANGE_ME: bucket_name, environment tags

resource "aws_s3_bucket" "data_bucket" {
  bucket = var.bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_bucket.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "sse" {
  bucket = aws_s3_bucket.data_bucket.id
  rule { apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" } }
}

resource "aws_s3_bucket_public_access_block" "block" {
  bucket                  = aws_s3_bucket.data_bucket.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

variable "bucket_name" { type = string }
variable "tags" { type = map(string); default = {} }
output "bucket_arn" { value = aws_s3_bucket.data_bucket.arn }
output "bucket_name" { value = aws_s3_bucket.data_bucket.id }
