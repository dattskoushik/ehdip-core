-- Day 8: Snowflake Storage Integration, External Volumes, and Dynamic Tables

USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS EHDIP_PROD;
USE DATABASE EHDIP_PROD;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

-- 1. Create Storage Integration for AWS S3
CREATE OR REPLACE STORAGE INTEGRATION s3_ehdip_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_s3_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ehdip-silver-data-lake-123456789012/', 's3://ehdip-gold-data-lake-123456789012/');

-- 2. Create Catalog Integration for AWS Glue
CREATE OR REPLACE CATALOG INTEGRATION glue_ehdip_catalog
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'ehdip_silver'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_glue_role'
  GLUE_CATALOG_ID = '123456789012'
  ENABLED = TRUE;

-- 3. Create External Volume for Iceberg Storage
CREATE OR REPLACE EXTERNAL VOLUME ehdip_ext_vol
  STORAGE_LOCATIONS = (
    (
      NAME = 'silver_s3'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://ehdip-silver-data-lake-123456789012/iceberg/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_s3_role'
    )
  );

-- 4. Create Iceberg Table referencing Silver Data Lake
CREATE OR REPLACE ICEBERG TABLE SILVER.omop_condition_occurrence
  EXTERNAL_VOLUME = 'ehdip_ext_vol'
  CATALOG = 'glue_ehdip_catalog'
  CATALOG_TABLE_NAME = 'omop_condition_occurrence';

-- 5. Create Dynamic Tables with Target Lags
-- Silver Dynamic Table (5 minute lag) for near real-time views
CREATE OR REPLACE DYNAMIC TABLE SILVER.dt_active_conditions
  TARGET_LAG = '5 minutes'
  WAREHOUSE = 'COMPUTE_WH'
  AS
    SELECT
      condition_occurrence_id,
      person_id,
      condition_concept_id,
      condition_start_date
    FROM SILVER.omop_condition_occurrence
    WHERE condition_end_date IS NULL;

-- Gold Dynamic Table (60 minute lag) for aggregated metrics
CREATE OR REPLACE DYNAMIC TABLE GOLD.dt_daily_condition_stats
  TARGET_LAG = '60 minutes'
  WAREHOUSE = 'COMPUTE_WH'
  AS
    SELECT
      condition_concept_id,
      DATE_TRUNC('DAY', condition_start_date) AS condition_date,
      COUNT(DISTINCT person_id) AS unique_patients
    FROM SILVER.dt_active_conditions
    GROUP BY 1, 2;
