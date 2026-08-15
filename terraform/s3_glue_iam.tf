# terraform/s3_glue_iam.tf

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# KMS Key for S3 Encryption (HIPAA Requirement)
resource "aws_kms_key" "datalake_kms" {
  description             = "KMS key for encrypting Datalake S3 buckets"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      }
    ]
  })
}

resource "aws_kms_alias" "datalake_kms_alias" {
  name          = "alias/datalake-key"
  target_key_id = aws_kms_key.datalake_kms.key_id
}

# S3 Buckets for Medallion Architecture
locals {
  buckets = ["bronze", "silver", "gold"]
}

resource "aws_s3_bucket" "datalake_buckets" {
  for_each = toset(local.buckets)
  bucket   = "ehdip-datalake-${each.key}-${data.aws_caller_identity.current.account_id}"

  tags = {
    Environment = "Production"
    Zone        = each.key
    Compliance  = "HIPAA"
  }
}

# Block Public Access (HIPAA Requirement)
resource "aws_s3_bucket_public_access_block" "datalake_public_access_block" {
  for_each = aws_s3_bucket.datalake_buckets

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable SSE-KMS Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "datalake_encryption" {
  for_each = aws_s3_bucket.datalake_buckets

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.datalake_kms.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

# Enable Versioning
resource "aws_s3_bucket_versioning" "datalake_versioning" {
  for_each = aws_s3_bucket.datalake_buckets

  bucket = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Data AWS Caller Identity
data "aws_caller_identity" "current" {}

# Glue Catalog Database
resource "aws_glue_catalog_database" "iceberg_catalog" {
  name        = "ehdip_iceberg_catalog"
  description = "Glue Catalog for Iceberg Tables"
}

# Least Privilege IAM Role for Data Engineering Processing
resource "aws_iam_role" "de_processing_role" {
  name = "EHDIP_DE_Processing_Role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = ["glue.amazonaws.com", "emr-serverless.amazonaws.com"]
        }
      }
    ]
  })
}

resource "aws_iam_policy" "de_processing_policy" {
  name        = "EHDIP_DE_Processing_Policy"
  description = "Least privilege access for DE processing"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = flatten([
          for bucket in aws_s3_bucket.datalake_buckets : [
            bucket.arn,
            "${bucket.arn}/*"
          ]
        ])
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey"
        ]
        Resource = aws_kms_key.datalake_kms.arn
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable"
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:database/ehdip_iceberg_catalog",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/ehdip_iceberg_catalog/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "de_processing_attach" {
  role       = aws_iam_role.de_processing_role.name
  policy_arn = aws_iam_policy.de_processing_policy.arn
}
