-- Day 8: Snowflake Storage Integration & Dynamic Tables

USE ROLE ACCOUNTADMIN;

-- 1. Create Storage Integration for AWS S3
CREATE OR REPLACE STORAGE INTEGRATION ehdip_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/ehdip-snowflake-access-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ehdip-data-lake-silver/', 's3://ehdip-data-lake-gold/');

-- 2. Create External Volume for Iceberg
CREATE OR REPLACE EXTERNAL VOLUME ehdip_iceberg_vol
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'us-east-1-silver'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://ehdip-data-lake-silver/iceberg/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/ehdip-snowflake-access-role'
         )
      );

-- 3. Create Catalog Integration for AWS Glue
CREATE OR REPLACE CATALOG INTEGRATION ehdip_glue_catalog_int
  CATALOG_SOURCE=GLUE
  CATALOG_NAMESPACE='ehdip_silver'
  TABLE_FORMAT=ICEBERG
  GLUE_AWS_ROLE_ARN='arn:aws:iam::123456789012:role/ehdip-snowflake-glue-role'
  GLUE_CATALOG_ID='123456789012'
  GLUE_REGION='us-east-1'
  ENABLED=TRUE;

-- 4. Create Iceberg Table referencing external S3/Glue data
USE DATABASE EHDIP_PROD;
USE SCHEMA SILVER;

CREATE OR REPLACE ICEBERG TABLE condition_occurrence
  EXTERNAL_VOLUME = 'ehdip_iceberg_vol'
  CATALOG = 'ehdip_glue_catalog_int'
  CATALOG_TABLE_NAME = 'condition_occurrence';

-- 5. Create Dynamic Table with 60-minute target lag (Gold Layer)
USE SCHEMA GOLD;

CREATE OR REPLACE DYNAMIC TABLE dt_patient_condition_summary
  TARGET_LAG = '60 minutes'
  WAREHOUSE = EHDIP_COMPUTE_WH
  AS
    SELECT
      person_id,
      COUNT(DISTINCT condition_concept_id) as unique_conditions,
      MAX(condition_start_date) as last_diagnosis_date
    FROM EHDIP_PROD.SILVER.condition_occurrence
    GROUP BY person_id;
