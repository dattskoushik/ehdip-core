# terraform/s3_glue_iam.tf
provider "aws" {
  region = "us-east-1"
}

# KMS Key for S3 SSE
resource "aws_kms_key" "ehdip_key" {
  description             = "KMS key for EHDIP S3 buckets"
  deletion_window_in_days = 10
  enable_key_rotation     = true
}

# S3 Buckets for Medallion Architecture
resource "aws_s3_bucket" "bronze" {
  bucket = "ehdip-bronze-data-lake"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze_sse" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.ehdip_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket" "silver" {
  bucket = "ehdip-silver-data-lake"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver_sse" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.ehdip_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket" "gold" {
  bucket = "ehdip-gold-data-lake"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold_sse" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.ehdip_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

# AWS Glue Catalog Database
resource "aws_glue_catalog_database" "ehdip_catalog" {
  name = "ehdip_catalog"
}

# IAM Role for least-privilege access
resource "aws_iam_role" "ehdip_data_engineer_role" {
  name = "ehdip_data_engineer_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "ehdip_s3_access_policy" {
  name        = "ehdip_s3_access_policy"
  description = "Least-privilege policy for S3 access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*"
        ]
      },
      {
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Effect = "Allow"
        Resource = aws_kms_key.ehdip_key.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ehdip_role_policy_attach" {
  role       = aws_iam_role.ehdip_data_engineer_role.name
  policy_arn = aws_iam_policy.ehdip_s3_access_policy.arn
}
