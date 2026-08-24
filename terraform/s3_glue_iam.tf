provider "aws" {
  region = "us-east-1"
}

# KMS Key for S3 Encryption (HIPAA requirement)
resource "aws_kms_key" "s3_kms_key" {
  description             = "KMS key for EHDIP S3 buckets encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

# S3 Buckets
resource "aws_s3_bucket" "bronze" {
  bucket = "ehdip-bronze-raw"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze_sse" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3_kms_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket" "silver" {
  bucket = "ehdip-silver-standardized"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver_sse" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3_kms_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket" "gold" {
  bucket = "ehdip-gold-marts"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold_sse" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3_kms_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

# Block Public Access for HIPAA compliance
resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# AWS Glue Catalog
resource "aws_glue_catalog_database" "ehdip_catalog" {
  name = "ehdip_data_lake"
}

# IAM Policies (Least-Privilege)
resource "aws_iam_policy" "ehdip_s3_least_privilege" {
  name        = "ehdip_s3_least_privilege"
  description = "Least privilege IAM policy for EHDIP S3 access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Effect = "Allow"
        Resource = [
          "${aws_s3_bucket.bronze.arn}/*",
          "${aws_s3_bucket.silver.arn}/*",
          "${aws_s3_bucket.gold.arn}/*"
        ]
      },
      {
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Effect   = "Allow"
        Resource = aws_kms_key.s3_kms_key.arn
      }
    ]
  })
}

resource "aws_iam_role" "ehdip_data_platform_role" {
  name = "ehdip_data_platform_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ehdip_s3_least_privilege_attachment" {
  role       = aws_iam_role.ehdip_data_platform_role.name
  policy_arn = aws_iam_policy.ehdip_s3_least_privilege.arn
}
