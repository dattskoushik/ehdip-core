provider "aws" {
  region = "us-east-1"
}

# KMS Key for S3 Encryption (HIPAA requirement)
resource "aws_kms_key" "ehdip_s3_key" {
  description             = "KMS key for EHDIP S3 buckets (Bronze, Silver, Gold)"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_kms_alias" "ehdip_s3_key_alias" {
  name          = "alias/ehdip-s3-key"
  target_key_id = aws_kms_key.ehdip_s3_key.key_id
}

# S3 Buckets for Medallion Architecture
resource "aws_s3_bucket" "bronze" {
  bucket = "ehdip-bronze-data-lake"
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

resource "aws_s3_bucket_public_access_block" "bronze_public_access" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "silver" {
  bucket = "ehdip-silver-data-lake"
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

resource "aws_s3_bucket_public_access_block" "silver_public_access" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "gold" {
  bucket = "ehdip-gold-data-lake"
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

resource "aws_s3_bucket_public_access_block" "gold_public_access" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# AWS Glue Catalog Databases
resource "aws_glue_catalog_database" "bronze_db" {
  name = "ehdip_bronze_db"
  location_uri = "s3://${aws_s3_bucket.bronze.bucket}/"
}

resource "aws_glue_catalog_database" "silver_db" {
  name = "ehdip_silver_db"
  location_uri = "s3://${aws_s3_bucket.silver.bucket}/"
}

resource "aws_glue_catalog_database" "gold_db" {
  name = "ehdip_gold_db"
  location_uri = "s3://${aws_s3_bucket.gold.bucket}/"
}

# IAM Role for EMR Serverless Execution (Least-Privilege)
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

# Least-Privilege IAM Policy for EMR Serverless Execution Role
resource "aws_iam_policy" "emr_serverless_policy" {
  name        = "ehdip-emr-serverless-execution-policy"
  description = "HIPAA-compliant least-privilege policy for EMR Serverless accessing S3, Glue, and KMS"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3BucketAccess"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.bronze.arn,
          aws_s3_bucket.silver.arn,
          aws_s3_bucket.gold.arn
        ]
      },
      {
        Sid    = "S3ObjectAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${aws_s3_bucket.bronze.arn}/*",
          "${aws_s3_bucket.silver.arn}/*",
          "${aws_s3_bucket.gold.arn}/*"
        ]
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateTable",
          "glue:GetTable",
          "glue:GetTables",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartitions"
        ]
        Resource = [
          "arn:aws:glue:us-east-1:*:catalog",
          "arn:aws:glue:us-east-1:*:database/ehdip_bronze_db",
          "arn:aws:glue:us-east-1:*:database/ehdip_silver_db",
          "arn:aws:glue:us-east-1:*:database/ehdip_gold_db",
          "arn:aws:glue:us-east-1:*:table/ehdip_bronze_db/*",
          "arn:aws:glue:us-east-1:*:table/ehdip_silver_db/*",
          "arn:aws:glue:us-east-1:*:table/ehdip_gold_db/*"
        ]
      },
      {
        Sid    = "KMSAccess"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = [
          aws_kms_key.ehdip_s3_key.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_serverless_role_attach" {
  role       = aws_iam_role.emr_serverless_role.name
  policy_arn = aws_iam_policy.emr_serverless_policy.arn
}
