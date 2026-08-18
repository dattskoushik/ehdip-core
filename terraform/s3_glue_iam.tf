provider "aws" {
  region = "us-east-1"
}

# KMS Key for S3 SSE
resource "aws_kms_key" "ehdip_s3_kms" {
  description             = "KMS key for EHDIP S3 bucket SSE"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  policy                  = data.aws_iam_policy_document.kms_policy.json
}

data "aws_iam_policy_document" "kms_policy" {
  statement {
    sid       = "Enable IAM User Permissions"
    effect    = "Allow"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }
}

data "aws_caller_identity" "current" {}

# S3 Buckets for Medallion Architecture
resource "aws_s3_bucket" "ehdip_bronze" {
  bucket = "ehdip-bronze-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ehdip_bronze_sse" {
  bucket = aws_s3_bucket.ehdip_bronze.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.ehdip_s3_kms.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket" "ehdip_silver" {
  bucket = "ehdip-silver-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ehdip_silver_sse" {
  bucket = aws_s3_bucket.ehdip_silver.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.ehdip_s3_kms.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket" "ehdip_gold" {
  bucket = "ehdip-gold-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ehdip_gold_sse" {
  bucket = aws_s3_bucket.ehdip_gold.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.ehdip_s3_kms.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

# AWS Glue Catalog Databases
resource "aws_glue_catalog_database" "ehdip_bronze_db" {
  name = "ehdip_bronze"
}

resource "aws_glue_catalog_database" "ehdip_silver_db" {
  name = "ehdip_silver"
}

resource "aws_glue_catalog_database" "ehdip_gold_db" {
  name = "ehdip_gold"
}

# IAM Role for Processing (Least Privilege)
resource "aws_iam_role" "ehdip_processing_role" {
  name = "EHDIPProcessingRole"

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
  name        = "EHDIPS3AccessPolicy"
  description = "Allows access to EHDIP Medallion buckets"
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
        Resource = [
          "${aws_s3_bucket.ehdip_bronze.arn}",
          "${aws_s3_bucket.ehdip_bronze.arn}/*",
          "${aws_s3_bucket.ehdip_silver.arn}",
          "${aws_s3_bucket.ehdip_silver.arn}/*",
          "${aws_s3_bucket.ehdip_gold.arn}",
          "${aws_s3_bucket.ehdip_gold.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey"
        ]
        Resource = aws_kms_key.ehdip_s3_kms.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ehdip_processing_attach" {
  role       = aws_iam_role.ehdip_processing_role.name
  policy_arn = aws_iam_policy.ehdip_s3_access_policy.arn
}
