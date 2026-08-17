# Day 1: Infrastructure Foundation & Iceberg Storage Setup

provider "aws" {
  region = "us-east-1"
}

# KMS Key for SSE-KMS
resource "aws_kms_key" "ehdip_s3_key" {
  description             = "KMS key for EHDIP S3 buckets SSE"
  deletion_window_in_days = 10
  enable_key_rotation     = true
}

# S3 Buckets for Medallion Architecture (Bronze, Silver, Gold)
resource "aws_s3_bucket" "bronze" {
  bucket = "ehdip-data-lake-bronze"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze_sse" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.ehdip_s3_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket" "silver" {
  bucket = "ehdip-data-lake-silver"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver_sse" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.ehdip_s3_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket" "gold" {
  bucket = "ehdip-data-lake-gold"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold_sse" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.ehdip_s3_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

# AWS Glue Catalog Databases
resource "aws_glue_catalog_database" "bronze_db" {
  name = "ehdip_bronze"
  location_uri = "s3://${aws_s3_bucket.bronze.bucket}/"
}

resource "aws_glue_catalog_database" "silver_db" {
  name = "ehdip_silver"
  location_uri = "s3://${aws_s3_bucket.silver.bucket}/"
}

resource "aws_glue_catalog_database" "gold_db" {
  name = "ehdip_gold"
  location_uri = "s3://${aws_s3_bucket.gold.bucket}/"
}

# HIPAA Least-Privilege IAM Policies for Data Engineers
data "aws_iam_policy_document" "data_engineer_s3_policy_doc" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]
    resources = [
      aws_s3_bucket.bronze.arn,
      "${aws_s3_bucket.bronze.arn}/*",
      aws_s3_bucket.silver.arn,
      "${aws_s3_bucket.silver.arn}/*",
      aws_s3_bucket.gold.arn,
      "${aws_s3_bucket.gold.arn}/*"
    ]
  }

  statement {
    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey"
    ]
    resources = [aws_kms_key.ehdip_s3_key.arn]
  }

  statement {
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:UpdateTable",
      "glue:DeleteTable"
    ]
    resources = ["*"] # Narrow this down to specific catalog/db ARNs in production
  }
}

resource "aws_iam_policy" "data_engineer_policy" {
  name        = "ehdip-data-engineer-policy"
  description = "Least privilege policy for data engineers accessing EHDIP data lake"
  policy      = data_aws_iam_policy_document.data_engineer_s3_policy_doc.json
}
