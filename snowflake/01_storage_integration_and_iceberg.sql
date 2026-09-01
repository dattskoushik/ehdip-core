-- 1. Create Storage Integration for AWS S3
CREATE OR REPLACE STORAGE INTEGRATION ehdip_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-s3-access-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ehdip-silver-zone/', 's3://ehdip-gold-zone/');

-- 2. Create Catalog Integration for AWS Glue
CREATE OR REPLACE CATALOG INTEGRATION ehdip_glue_int
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'ehdip_silver_db'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-glue-access-role'
  GLUE_CATALOG_ID = '123456789012'
  ENABLED = TRUE;

-- 3. Create External Volume for Iceberg Storage
CREATE OR REPLACE EXTERNAL VOLUME ehdip_ext_vol
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'ehdip-silver-us-east-1'
        STORAGE_PROVIDER = 'S3'
        STORAGE_BASE_URL = 's3://ehdip-silver-zone/'
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-s3-access-role'
      )
    );

-- 4. Create Iceberg Table linked to Silver Zone
CREATE OR REPLACE ICEBERG TABLE condition_occurrence
  EXTERNAL_VOLUME = 'ehdip_ext_vol'
  CATALOG = 'ehdip_glue_int'
  CATALOG_TABLE_NAME = 'condition_occurrence';

-- 5. Create Dynamic Tables with Target Lags
-- Silver Dynamic Table (5 min lag)
CREATE OR REPLACE DYNAMIC TABLE condition_occurrence_silver_dt
  TARGET_LAG = '5 minutes'
  WAREHOUSE = ehdip_wh
  AS
  SELECT * FROM condition_occurrence;

-- Gold Dynamic Table (60 min lag)
CREATE OR REPLACE DYNAMIC TABLE readmissions_30_day_dt
  TARGET_LAG = '60 minutes'
  WAREHOUSE = ehdip_wh
  AS
  SELECT
    person_id,
    COUNT(condition_occurrence_id) as admission_count
  FROM condition_occurrence_silver_dt
  -- Simulation of a readmissions aggregate logic
  GROUP BY person_id;
