-- snowflake/01_storage_integration_and_iceberg.sql
USE ROLE ACCOUNTADMIN;

-- Create Storage Integration for AWS S3
CREATE STORAGE INTEGRATION ehdip_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/ehdip_snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ehdip-silver-data-lake/', 's3://ehdip-gold-data-lake/');

-- Create External Volume for Iceberg
CREATE EXTERNAL VOLUME ehdip_iceberg_vol
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'us-east-1-silver'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://ehdip-silver-data-lake/iceberg/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/ehdip_snowflake_role'
         )
      );

-- Create Catalog Integration for AWS Glue
CREATE CATALOG INTEGRATION ehdip_glue_cat
  CATALOG_SOURCE=GLUE
  CATALOG_NAMESPACE='ehdip_catalog.ehdip'
  TABLE_FORMAT=ICEBERG
  GLUE_AWS_ROLE_ARN='arn:aws:iam::123456789012:role/ehdip_snowflake_role'
  GLUE_CATALOG_ID='123456789012'
  GLUE_REGION='us-east-1';

-- Create database and schema
CREATE DATABASE IF NOT EXISTS EHDIP_DB;
CREATE SCHEMA IF NOT EXISTS EHDIP_DB.SILVER;
CREATE SCHEMA IF NOT EXISTS EHDIP_DB.GOLD;

-- Mount Iceberg Table in Snowflake (Unmanaged / External)
CREATE ICEBERG TABLE EHDIP_DB.SILVER.condition_occurrence
  EXTERNAL_VOLUME = 'ehdip_iceberg_vol'
  CATALOG = 'ehdip_glue_cat'
  CATALOG_TABLE_NAME = 'silver_condition_occurrence';

-- Create Dynamic Table for 5 min Silver updates (Example: Aggregated counts)
CREATE OR REPLACE DYNAMIC TABLE EHDIP_DB.SILVER.dt_condition_counts
  TARGET_LAG = '5 minutes'
  WAREHOUSE = EHDIP_WH
  AS
    SELECT condition_concept_id, count(*) as condition_count, max(etl_timestamp) as last_updated
    FROM EHDIP_DB.SILVER.condition_occurrence
    GROUP BY condition_concept_id;

-- Create Dynamic Table for 60 min Gold updates
CREATE OR REPLACE DYNAMIC TABLE EHDIP_DB.GOLD.dt_patient_conditions
  TARGET_LAG = '60 minutes'
  WAREHOUSE = EHDIP_WH
  AS
    SELECT person_id, count(condition_concept_id) as total_conditions
    FROM EHDIP_DB.SILVER.condition_occurrence
    GROUP BY person_id;
