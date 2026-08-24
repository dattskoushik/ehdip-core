-- 1. Create Storage Integration for AWS S3
CREATE OR REPLACE STORAGE INTEGRATION ehdip_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/ehdip-snowflake-access-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ehdip-silver-data-lake/', 's3://ehdip-gold-data-lake/');

-- 2. Create External Volume for Iceberg
CREATE OR REPLACE EXTERNAL VOLUME ehdip_iceberg_vol
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'us-east-1-silver'
        STORAGE_PROVIDER = 'S3'
        STORAGE_BASE_URL = 's3://ehdip-silver-data-lake/'
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/ehdip-snowflake-access-role'
      )
    );

-- 3. Create Catalog Integration for AWS Glue
CREATE OR REPLACE CATALOG INTEGRATION ehdip_glue_catalog_int
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'ehdip_silver_db'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/ehdip-snowflake-glue-role'
  GLUE_CATALOG_ID = '123456789012'
  GLUE_REGION = 'us-east-1'
  ENABLED = TRUE;

-- 4. Create Iceberg Table pointing to Silver (Managed externally by Spark/Glue)
CREATE OR REPLACE ICEBERG TABLE silver_condition_occurrence
  EXTERNAL_VOLUME = 'ehdip_iceberg_vol'
  CATALOG = 'ehdip_glue_catalog_int'
  CATALOG_TABLE_NAME = 'condition_occurrence';

CREATE OR REPLACE ICEBERG TABLE silver_drug_exposure
  EXTERNAL_VOLUME = 'ehdip_iceberg_vol'
  CATALOG = 'ehdip_glue_catalog_int'
  CATALOG_TABLE_NAME = 'drug_exposure';

-- 5. Create Dynamic Tables for downstream Gold Marts
-- 5-minute lag for Silver views
CREATE OR REPLACE DYNAMIC TABLE dt_silver_patient_events
  TARGET_LAG = '5 minutes'
  WAREHOUSE = 'EHDIP_TRANSFORM_WH'
  AS
    SELECT
      c.person_id,
      c.condition_concept_id as event_concept_id,
      c.condition_start_date as event_date,
      'CONDITION' as event_type
    FROM silver_condition_occurrence c
    UNION ALL
    SELECT
      d.person_id,
      d.drug_concept_id as event_concept_id,
      d.drug_exposure_start_date as event_date,
      'DRUG' as event_type
    FROM silver_drug_exposure d;

-- 60-minute lag for Gold summary
CREATE OR REPLACE DYNAMIC TABLE dt_gold_patient_summary
  TARGET_LAG = '60 minutes'
  WAREHOUSE = 'EHDIP_TRANSFORM_WH'
  AS
    SELECT
      person_id,
      COUNT(CASE WHEN event_type = 'CONDITION' THEN 1 END) as total_conditions,
      COUNT(CASE WHEN event_type = 'DRUG' THEN 1 END) as total_drugs,
      MAX(event_date) as last_event_date
    FROM dt_silver_patient_events
    GROUP BY person_id;
