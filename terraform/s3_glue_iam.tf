terraform {
  required_version = ">= 1.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# KMS Key for S3 Encryption (HIPAA requirement)
resource "aws_kms_key" "ehdip_s3_key" {
  description             = "KMS key for EHDIP S3 buckets (HIPAA compliance)"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "ehdip_s3_key_alias" {
  name          = "alias/ehdip-s3-kms"
  target_key_id = aws_kms_key.ehdip_s3_key.key_id
}

# Data Lake Buckets (Bronze, Silver, Gold)
resource "aws_s3_bucket" "ehdip_data_lake" {
  for_each = toset(["bronze", "silver", "gold"])
  bucket   = "ehdip-${each.key}-data-lake-${data.aws_caller_identity.current.account_id}"
}

data "aws_caller_identity" "current" {}

resource "aws_s3_bucket_server_side_encryption_configuration" "ehdip_encryption" {
  for_each = aws_s3_bucket.ehdip_data_lake

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.ehdip_s3_key.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "ehdip_public_access_block" {
  for_each = aws_s3_bucket.ehdip_data_lake

  bucket = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# AWS Glue Catalog Databases
resource "aws_glue_catalog_database" "ehdip_db" {
  for_each = toset(["bronze", "silver", "gold"])
  name     = "ehdip_${each.key}"
}

# IAM Role for EMR Serverless / PySpark
resource "aws_iam_role" "emr_serverless_role" {
  name = "ehdip-emr-serverless-execution-role"

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

# IAM Policy for S3, Glue, and KMS access (Least Privilege)
resource "aws_iam_policy" "emr_serverless_policy" {
  name        = "ehdip-emr-serverless-policy"
  description = "Least privilege policy for EMR Serverless (S3, Glue, KMS)"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowS3List"
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [for bucket in aws_s3_bucket.ehdip_data_lake : bucket.arn]
      },
      {
        Sid    = "AllowS3ObjectAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [for bucket in aws_s3_bucket.ehdip_data_lake : "${bucket.arn}/*"]
      },
      {
        Sid    = "AllowGlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateTable",
          "glue:GetTable",
          "glue:GetTables",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchDeletePartition"
        ]
        Resource = flatten([
          [for db in aws_glue_catalog_database.ehdip_db : "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog"],
          [for db in aws_glue_catalog_database.ehdip_db : "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:database/${db.name}"],
          [for db in aws_glue_catalog_database.ehdip_db : "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/${db.name}/*"]
        ])
      },
      {
        Sid    = "AllowKMSAccess"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = aws_kms_key.ehdip_s3_key.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_serverless_attach" {
  role       = aws_iam_role.emr_serverless_role.name
  policy_arn = aws_iam_policy.emr_serverless_policy.arn
}
