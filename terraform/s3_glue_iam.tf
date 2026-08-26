provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "us-east-1"
}

variable "project_prefix" {
  default = "ehdip"
}

# KMS Key for S3 SSE
resource "aws_kms_key" "ehdip_s3_key" {
  description             = "KMS key for EHDIP S3 buckets (HIPAA compliant)"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

# S3 Buckets
resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_prefix}-bronze-data-${var.aws_region}"
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

resource "aws_s3_bucket_public_access_block" "bronze_public_block" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "silver" {
  bucket = "${var.project_prefix}-silver-data-${var.aws_region}"
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

resource "aws_s3_bucket_public_access_block" "silver_public_block" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "gold" {
  bucket = "${var.project_prefix}-gold-data-${var.aws_region}"
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

resource "aws_s3_bucket_public_access_block" "gold_public_block" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# AWS Glue Catalog Databases
resource "aws_glue_catalog_database" "bronze_db" {
  name = "${var.project_prefix}_bronze_db"
}

resource "aws_glue_catalog_database" "silver_db" {
  name = "${var.project_prefix}_silver_db"
}

resource "aws_glue_catalog_database" "gold_db" {
  name = "${var.project_prefix}_gold_db"
}

# EMR Serverless Execution Role
resource "aws_iam_role" "emr_serverless_role" {
  name = "${var.project_prefix}_emr_serverless_exec_role"
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

# IAM Policy for S3 and Glue access
resource "aws_iam_policy" "ehdip_data_access_policy" {
  name        = "${var.project_prefix}_data_access_policy"
  description = "HIPAA least-privilege IAM policy for S3 and Glue"
  policy      = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [
          aws_s3_bucket.bronze.arn,
          aws_s3_bucket.silver.arn,
          aws_s3_bucket.gold.arn
        ]
      },
      {
        Effect   = "Allow"
        Action   = [
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
        Effect   = "Allow"
        Action   = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = [aws_kms_key.ehdip_s3_key.arn]
      },
      {
        Effect   = "Allow"
        Action   = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:UpdateDatabase",
          "glue:DeleteDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchGetPartition"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.bronze_db.name}",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.bronze_db.name}/*",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.silver_db.name}",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.silver_db.name}/*",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.gold_db.name}",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.gold_db.name}/*"
        ]
      }
    ]
  })
}

data "aws_caller_identity" "current" {}

resource "aws_iam_role_policy_attachment" "emr_policy_attach" {
  role       = aws_iam_role.emr_serverless_role.name
  policy_arn = aws_iam_policy.ehdip_data_access_policy.arn
}
