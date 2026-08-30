provider "aws" {
  region = "us-east-1"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

resource "aws_kms_key" "ehdip_kms" {
  description             = "KMS key for EHDIP storage encryption"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

locals {
  zones = ["bronze", "silver", "gold"]
}

resource "aws_s3_bucket" "ehdip_datalake" {
  count  = length(local.zones)
  bucket = "ehdip-datalake-${local.zones[count.index]}-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ehdip_sse" {
  count  = length(local.zones)
  bucket = aws_s3_bucket.ehdip_datalake[count.index].id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.ehdip_kms.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "ehdip_pab" {
  count  = length(local.zones)
  bucket = aws_s3_bucket.ehdip_datalake[count.index].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_glue_catalog_database" "ehdip_db" {
  count = length(local.zones)
  name  = "ehdip_${local.zones[count.index]}_db"
}

resource "aws_iam_role" "emr_serverless_execution_role" {
  name = "EhdipEmrServerlessExecutionRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "emr-serverless.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "emr_serverless_s3_glue_policy" {
  name        = "EhdipEmrServerlessS3GluePolicy"
  description = "Least privilege policy for EMR Serverless to access S3 and Glue"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [for b in aws_s3_bucket.ehdip_datalake : b.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [for b in aws_s3_bucket.ehdip_datalake : "${b.arn}/*"]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:CreateDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition"
        ]
        Resource = concat(
          ["arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog"],
          [for db in aws_glue_catalog_database.ehdip_db : "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${db.name}"],
          [for db in aws_glue_catalog_database.ehdip_db : "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${db.name}/*"]
        )
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = aws_kms_key.ehdip_kms.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_attach" {
  role       = aws_iam_role.emr_serverless_execution_role.name
  policy_arn = aws_iam_policy.emr_serverless_s3_glue_policy.arn
}
