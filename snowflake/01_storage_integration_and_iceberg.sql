-- Create Storage Integration for AWS S3
CREATE OR REPLACE STORAGE INTEGRATION s3_ehdip_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeS3Role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ehdip-datalake-silver-123456789012/', 's3://ehdip-datalake-gold-123456789012/');

-- Create External Volume for Iceberg
CREATE OR REPLACE EXTERNAL VOLUME exvol_ehdip_silver
  STORAGE_LOCATIONS = (
    (
      NAME = 'silver_s3'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://ehdip-datalake-silver-123456789012/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeIcebergRole'
    )
  );

-- Create Iceberg Table pointing to Glue/S3 (Unmanaged/External)
CREATE OR REPLACE ICEBERG TABLE ehdip_silver.condition_occurrence
  EXTERNAL_VOLUME = 'exvol_ehdip_silver'
  CATALOG = 'AWS_GLUE'
  CATALOG_TABLE_NAME = 'condition_occurrence'
  CATALOG_NAMESPACE = 'ehdip_silver_db';

-- Create Dynamic Table for 5-minute Silver refresh
CREATE OR REPLACE DYNAMIC TABLE ehdip_silver.dt_condition_occurrence
  TARGET_LAG = '5 minutes'
  WAREHOUSE = 'ehdip_transform_wh'
  AS
    SELECT * FROM ehdip_silver.condition_occurrence;

-- Create Dynamic Table for 60-minute Gold refresh (pre-computation for marts)
CREATE OR REPLACE DYNAMIC TABLE ehdip_gold.dt_readmissions_base
  TARGET_LAG = '60 minutes'
  WAREHOUSE = 'ehdip_transform_wh'
  AS
    SELECT
        c.person_id,
        c.condition_start_date,
        c.condition_source_value
    FROM ehdip_silver.dt_condition_occurrence c;
