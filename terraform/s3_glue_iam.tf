provider "aws" {
  region = "us-east-1"
}

data "aws_caller_identity" "current" {}

resource "aws_kms_key" "s3_kms_key" {
  description             = "KMS key for EHDIP S3 bucket encryption (HIPAA compliant)"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

locals {
  zones = ["bronze", "silver", "gold"]
}

resource "aws_s3_bucket" "datalake" {
  for_each = toset(local.zones)
  bucket   = "ehdip-datalake-${each.key}-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "datalake_versioning" {
  for_each = toset(local.zones)
  bucket   = aws_s3_bucket.datalake[each.key].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake_encryption" {
  for_each = toset(local.zones)
  bucket   = aws_s3_bucket.datalake[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3_kms_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "datalake_public_access" {
  for_each = toset(local.zones)
  bucket   = aws_s3_bucket.datalake[each.key].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_glue_catalog_database" "ehdip_database" {
  for_each = toset(local.zones)
  name     = "ehdip_${each.key}"
}

resource "aws_iam_role" "glue_service_role" {
  name = "ehdip-glue-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "glue.amazonaws.com",
            "emr-serverless.amazonaws.com"
          ]
        }
      }
    ]
  })
}

resource "aws_iam_policy" "least_privilege_policy" {
  name        = "ehdip-least-privilege-policy"
  description = "Least privilege IAM policy for EHDIP components"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          for zone in local.zones : "${aws_s3_bucket.datalake[zone].arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          for zone in local.zones : aws_s3_bucket.datalake[zone].arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.s3_kms_key.arn
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartitions",
          "glue:GetPartition",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition"
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:database/ehdip_bronze",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/ehdip_bronze/*",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:database/ehdip_silver",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/ehdip_silver/*",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:database/ehdip_gold",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/ehdip_gold/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_attach" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.least_privilege_policy.arn
}
