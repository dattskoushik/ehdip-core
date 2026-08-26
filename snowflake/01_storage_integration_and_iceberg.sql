-- 1. Create Storage Integration for AWS S3
CREATE STORAGE INTEGRATION s3_ehdip_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_ehdip_access_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ehdip-silver-data-us-east-1/', 's3://ehdip-gold-data-us-east-1/');

-- 2. Create Catalog Integration for AWS Glue
CREATE CATALOG INTEGRATION glue_ehdip_integration
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'ehdip_silver_db'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_ehdip_glue_role'
  GLUE_CATALOG_ID = '123456789012'
  ENABLED = TRUE;

-- 3. Create External Volume using the Storage Integration
CREATE EXTERNAL VOLUME ehdip_silver_vol
  STORAGE_LOCATIONS = (
    (
      NAME = 'us-east-1'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://ehdip-silver-data-us-east-1/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_ehdip_access_role'
    )
  );

-- 4. Create Snowflake Database and Schema
CREATE DATABASE IF NOT EXISTS EHDIP_PROD;
CREATE SCHEMA IF NOT EXISTS EHDIP_PROD.SILVER;
CREATE SCHEMA IF NOT EXISTS EHDIP_PROD.GOLD;

-- 5. Create Iceberg Tables linked to AWS Glue
CREATE ICEBERG TABLE EHDIP_PROD.SILVER.condition_occurrence
  EXTERNAL_VOLUME = 'ehdip_silver_vol'
  CATALOG = 'glue_ehdip_integration'
  CATALOG_TABLE_NAME = 'condition_occurrence';

CREATE ICEBERG TABLE EHDIP_PROD.SILVER.person
  EXTERNAL_VOLUME = 'ehdip_silver_vol'
  CATALOG = 'glue_ehdip_integration'
  CATALOG_TABLE_NAME = 'person';

-- 6. Create Dynamic Tables with Target Lags
-- Silver Dynamic Table (5 min lag)
CREATE DYNAMIC TABLE EHDIP_PROD.SILVER.dt_patient_conditions
  TARGET_LAG = '5 minutes'
  WAREHOUSE = 'COMPUTE_WH'
  AS
    SELECT
      p.person_source_value,
      p.birth_datetime,
      c.condition_concept_id,
      c.condition_start_date
    FROM EHDIP_PROD.SILVER.person p
    JOIN EHDIP_PROD.SILVER.condition_occurrence c
      ON p.person_source_value = c.person_source_value;

-- Gold Dynamic Table (60 min lag) - Aggregation example
CREATE DYNAMIC TABLE EHDIP_PROD.GOLD.dt_monthly_conditions
  TARGET_LAG = '60 minutes'
  WAREHOUSE = 'COMPUTE_WH'
  AS
    SELECT
      DATE_TRUNC('month', condition_start_date) as month,
      condition_concept_id,
      COUNT(DISTINCT person_source_value) as unique_patients
    FROM EHDIP_PROD.SILVER.dt_patient_conditions
    GROUP BY 1, 2;
