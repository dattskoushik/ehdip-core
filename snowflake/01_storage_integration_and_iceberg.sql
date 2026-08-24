-- Day 8: Snowflake Storage Integration & Dynamic Tables

-- 1. Create AWS IAM Storage Integration
CREATE STORAGE INTEGRATION s3_ehdip_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_ehdip_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ehdip-silver-standardized/', 's3://ehdip-gold-marts/');

-- 2. Create External Volume for Iceberg
CREATE EXTERNAL VOLUME ehdip_iceberg_vol
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'us-east-1-silver'
        STORAGE_PROVIDER = 'S3'
        STORAGE_BASE_URL = 's3://ehdip-silver-standardized/'
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_ehdip_role'
      )
    );

-- 3. Create External Catalog Integration (AWS Glue)
CREATE CATALOG INTEGRATION glue_ehdip_catalog
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'ehdip_data_lake'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_glue_role'
  GLUE_CATALOG_ID = '123456789012'
  GLUE_REGION = 'us-east-1'
  ENABLED = TRUE;

-- 4. Create Iceberg Table over Silver S3 data
CREATE ICEBERG TABLE silver_condition_occurrence
  EXTERNAL_VOLUME = 'ehdip_iceberg_vol'
  CATALOG = 'glue_ehdip_catalog'
  CATALOG_TABLE_NAME = 'condition_occurrence';

CREATE ICEBERG TABLE silver_drug_exposure
  EXTERNAL_VOLUME = 'ehdip_iceberg_vol'
  CATALOG = 'glue_ehdip_catalog'
  CATALOG_TABLE_NAME = 'drug_exposure';

-- 5. Create Dynamic Tables with Target Lags
-- 5 min lag for Silver intermediate models
CREATE DYNAMIC TABLE dt_silver_recent_conditions
  TARGET_LAG = '5 minutes'
  WAREHOUSE = 'ehdip_transform_wh'
  AS
  SELECT person_id, condition_concept_id, condition_start_date
  FROM silver_condition_occurrence
  WHERE condition_start_date >= CURRENT_DATE() - INTERVAL '30 DAYS';

-- 60 min lag for Gold Marts
CREATE DYNAMIC TABLE dt_gold_patient_summary
  TARGET_LAG = '60 minutes'
  WAREHOUSE = 'ehdip_transform_wh'
  AS
  SELECT
    person_id,
    COUNT(DISTINCT condition_concept_id) as total_conditions,
    MAX(condition_start_date) as last_condition_date
  FROM silver_condition_occurrence
  GROUP BY person_id;
