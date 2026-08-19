-- snowflake/01_storage_integration_and_iceberg.sql

-- 1. Create External Volume for Iceberg on S3
CREATE OR REPLACE EXTERNAL VOLUME ehdip_silver_ext_vol
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'us-east-1-silver-s3'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://ehdip-datalake-silver-123456789012/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/Snowflake_Iceberg_Access_Role'
         )
      );

-- 2. Create Catalog Integration for AWS Glue
CREATE OR REPLACE CATALOG INTEGRATION ehdip_glue_catalog_int
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'ehdip_iceberg_catalog.silver'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/Snowflake_Glue_Access_Role'
  GLUE_CATALOG_ID = '123456789012'
  GLUE_REGION = 'us-east-1'
  ENABLED = TRUE;

-- 3. Create Iceberg Tables linked to AWS Glue
CREATE DATABASE IF NOT EXISTS ehdip_db;
CREATE SCHEMA IF NOT EXISTS ehdip_db.silver;

USE SCHEMA ehdip_db.silver;

CREATE OR REPLACE ICEBERG TABLE omop_person
  EXTERNAL_VOLUME = 'ehdip_silver_ext_vol'
  CATALOG = 'ehdip_glue_catalog_int'
  CATALOG_TABLE_NAME = 'omop_person';

CREATE OR REPLACE ICEBERG TABLE omop_condition_occurrence
  EXTERNAL_VOLUME = 'ehdip_silver_ext_vol'
  CATALOG = 'ehdip_glue_catalog_int'
  CATALOG_TABLE_NAME = 'omop_condition_occurrence';

CREATE OR REPLACE ICEBERG TABLE omop_visit_occurrence
  EXTERNAL_VOLUME = 'ehdip_silver_ext_vol'
  CATALOG = 'ehdip_glue_catalog_int'
  CATALOG_TABLE_NAME = 'omop_visit_occurrence';

-- 4. Create Gold Schema and Dynamic Tables
CREATE SCHEMA IF NOT EXISTS ehdip_db.gold;
USE SCHEMA ehdip_db.gold;

-- Set up Warehouse for Dynamic Tables
CREATE OR REPLACE WAREHOUSE dt_wh WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- Example: Dynamic Table for 30-Day Readmission Metrics (60 min lag)
CREATE OR REPLACE DYNAMIC TABLE readmission_metrics
  TARGET_LAG = '60 minutes'
  WAREHOUSE = dt_wh
  AS
    WITH visits AS (
        SELECT
            person_id,
            visit_occurrence_id,
            visit_start_date,
            visit_end_date,
            LAG(visit_end_date) OVER (PARTITION BY person_id ORDER BY visit_start_date) as prev_discharge_date
        FROM ehdip_db.silver.omop_visit_occurrence
    )
    SELECT
        person_id,
        COUNT(visit_occurrence_id) as total_visits,
        SUM(CASE WHEN DATEDIFF('day', prev_discharge_date, visit_start_date) <= 30 THEN 1 ELSE 0 END) as readmissions_within_30_days
    FROM visits
    GROUP BY person_id;

-- Start the dynamic table
ALTER DYNAMIC TABLE readmission_metrics RESUME;
