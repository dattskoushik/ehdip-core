-- Snowflake Storage Integration for AWS S3
CREATE OR REPLACE STORAGE INTEGRATION ehdip_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeS3IntegrationRole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ehdip-silver/', 's3://ehdip-gold/');

-- AWS Glue Catalog Integration
CREATE OR REPLACE CATALOG INTEGRATION ehdip_glue_int
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'ehdip_silver'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeGlueIntegrationRole'
  GLUE_CATALOG_ID = '123456789012'
  ENABLED = TRUE;

-- External Volume Definition
CREATE OR REPLACE EXTERNAL VOLUME ehdip_silver_ext_vol
  STORAGE_LOCATIONS = (
    (
      NAME = 'us-east-1-silver'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://ehdip-silver/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeS3IntegrationRole'
    )
  );

-- Mount Silver Iceberg Table (Read-Only via Glue)
CREATE OR REPLACE ICEBERG TABLE ehdip.silver.omop_person
  EXTERNAL_VOLUME = 'ehdip_silver_ext_vol'
  CATALOG = 'ehdip_glue_int'
  BASE_LOCATION = 'omop_person';

-- Dynamic Table for Gold Layer (Continuous aggregation)
-- Target lag of 60 minutes
CREATE OR REPLACE DYNAMIC TABLE ehdip.gold.readmission_metrics
  TARGET_LAG = '60 minutes'
  WAREHOUSE = 'EHDIP_TRANSFORM_WH'
  AS
    SELECT
        p.gender_concept_id,
        COUNT(DISTINCT p.person_id) as total_patients,
        CURRENT_TIMESTAMP() as calculated_at
    FROM ehdip.silver.omop_person p
    GROUP BY p.gender_concept_id;
