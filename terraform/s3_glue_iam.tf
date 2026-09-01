provider "aws" {
  region = "us-east-1"
}

# KMS Key for S3 Encryption (HIPAA requirement)
resource "aws_kms_key" "ehdip_s3_key" {
  description             = "KMS key for EHDIP S3 buckets"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}

resource "aws_kms_alias" "ehdip_s3_key_alias" {
  name          = "alias/ehdip-s3-key"
  target_key_id = aws_kms_key.ehdip_s3_key.key_id
}

# S3 Buckets for Medallion Architecture
resource "aws_s3_bucket" "bronze" {
  bucket = "ehdip-bronze-zone"
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

resource "aws_s3_bucket_public_access_block" "bronze_block" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "silver" {
  bucket = "ehdip-silver-zone"
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

resource "aws_s3_bucket_public_access_block" "silver_block" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "gold" {
  bucket = "ehdip-gold-zone"
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

resource "aws_s3_bucket_public_access_block" "gold_block" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# AWS Glue Catalog Databases
resource "aws_glue_catalog_database" "ehdip_bronze_db" {
  name = "ehdip_bronze_db"
}

resource "aws_glue_catalog_database" "ehdip_silver_db" {
  name = "ehdip_silver_db"
}

resource "aws_glue_catalog_database" "ehdip_gold_db" {
  name = "ehdip_gold_db"
}

# IAM Role for EMR Serverless Execution
data "aws_iam_policy_document" "emr_serverless_trust_policy" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_serverless_execution_role" {
  name               = "ehdip-emr-serverless-role"
  assume_role_policy = data.aws_iam_policy_document.emr_serverless_trust_policy.json
}

# HIPAA Least-Privilege IAM Policy for EMR Serverless
data "aws_iam_policy_document" "emr_serverless_policy_doc" {
  # S3 Bucket Access
  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [
      aws_s3_bucket.bronze.arn,
      aws_s3_bucket.silver.arn,
      aws_s3_bucket.gold.arn
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${aws_s3_bucket.bronze.arn}/*",
      "${aws_s3_bucket.silver.arn}/*",
      "${aws_s3_bucket.gold.arn}/*"
    ]
  }

  # KMS Decrypt/Encrypt for S3
  statement {
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
      "kms:DescribeKey"
    ]
    resources = [aws_kms_key.ehdip_s3_key.arn]
  }

  # Glue Catalog Access scoped explicitly
  statement {
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:BatchCreatePartition"
    ]
    resources = [
      "arn:aws:glue:us-east-1:*:catalog",
      "arn:aws:glue:us-east-1:*:database/ehdip_bronze_db",
      "arn:aws:glue:us-east-1:*:table/ehdip_bronze_db/*",
      "arn:aws:glue:us-east-1:*:database/ehdip_silver_db",
      "arn:aws:glue:us-east-1:*:table/ehdip_silver_db/*",
      "arn:aws:glue:us-east-1:*:database/ehdip_gold_db",
      "arn:aws:glue:us-east-1:*:table/ehdip_gold_db/*"
    ]
  }
}

resource "aws_iam_role_policy" "emr_serverless_policy" {
  name   = "ehdip-emr-serverless-policy"
  role   = aws_iam_role.emr_serverless_execution_role.id
  policy = data.aws_iam_policy_document.emr_serverless_policy_doc.json
}
