-- Snowflake setup for AWS Glue Iceberg Integration and Dynamic Tables

USE ROLE ACCOUNTADMIN;

-- 1. Create Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION s3_ehdip_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_ehdip_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ehdip-datalake-silver/', 's3://ehdip-datalake-gold/');

-- 2. Create External Volume for Iceberg
CREATE OR REPLACE EXTERNAL VOLUME ehdip_iceberg_vol
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'us-east-1-silver'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://ehdip-datalake-silver/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_ehdip_role'
         )
      );

-- 3. Create Catalog Integration (AWS Glue)
CREATE OR REPLACE CATALOG INTEGRATION glue_catalog_int
  CATALOG_SOURCE=GLUE
  CATALOG_NAMESPACE='ehdip_silver'
  TABLE_FORMAT=ICEBERG
  GLUE_AWS_ROLE_ARN='arn:aws:iam::123456789012:role/snowflake_glue_role'
  GLUE_CATALOG_ID='123456789012'
  GLUE_REGION='us-east-1'
  ENABLED=TRUE;

-- 4. Create Database and Schema
CREATE DATABASE IF NOT EXISTS EHDIP_PROD;
CREATE SCHEMA IF NOT EXISTS EHDIP_PROD.SILVER;
CREATE SCHEMA IF NOT EXISTS EHDIP_PROD.GOLD;

USE SCHEMA EHDIP_PROD.SILVER;

-- 5. Create Iceberg Table linked to Silver S3
CREATE OR REPLACE ICEBERG TABLE condition_occurrence
  EXTERNAL_VOLUME = 'ehdip_iceberg_vol'
  CATALOG = 'glue_catalog_int'
  CATALOG_TABLE_NAME = 'condition_occurrence';

-- 6. Create Dynamic Table for initial Gold aggregation (5 minute lag from Silver)
USE SCHEMA EHDIP_PROD.GOLD;

CREATE OR REPLACE DYNAMIC TABLE condition_rollup_dt
  TARGET_LAG = '5 minutes'
  WAREHOUSE = 'EHDIP_WH'
  AS
    SELECT
      condition_concept_id,
      COUNT(condition_occurrence_id) as condition_count,
      MAX(condition_start_date) as last_seen_date
    FROM EHDIP_PROD.SILVER.condition_occurrence
    GROUP BY condition_concept_id;
