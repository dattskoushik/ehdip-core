-- snowflake/02_security_masking_rbac.sql

USE ROLE ACCOUNTADMIN;

-- 1. Create Roles
CREATE ROLE IF NOT EXISTS ehdip_data_engineer;
CREATE ROLE IF NOT EXISTS ehdip_data_scientist;
CREATE ROLE IF NOT EXISTS ehdip_clinical_analyst;

-- Grant usage on database and schemas
GRANT USAGE ON DATABASE ehdip_db TO ROLE ehdip_data_engineer;
GRANT USAGE ON SCHEMA ehdip_db.silver TO ROLE ehdip_data_engineer;
GRANT USAGE ON SCHEMA ehdip_db.gold TO ROLE ehdip_data_engineer;

GRANT USAGE ON DATABASE ehdip_db TO ROLE ehdip_data_scientist;
GRANT USAGE ON SCHEMA ehdip_db.silver TO ROLE ehdip_data_scientist;

GRANT USAGE ON DATABASE ehdip_db TO ROLE ehdip_clinical_analyst;
GRANT USAGE ON SCHEMA ehdip_db.gold TO ROLE ehdip_clinical_analyst;

-- 2. Create Masking Policies
CREATE OR REPLACE MASKING POLICY ehdip_db.silver.ssn_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('ehdip_data_engineer') THEN val
    ELSE '***-**-****'
  END;

CREATE OR REPLACE MASKING POLICY ehdip_db.silver.dob_mask AS (val date) RETURNS date ->
  CASE
    WHEN CURRENT_ROLE() IN ('ehdip_data_engineer', 'ehdip_clinical_analyst') THEN val
    ELSE DATE_TRUNC('YEAR', val) -- Mask to just the year for Data Scientists
  END;

-- 3. Apply Masking Policies to Tables
-- Assuming omop_person has these columns in a non-strict OMOP extension or source table
-- ALTER TABLE ehdip_db.silver.omop_person MODIFY COLUMN ssn SET MASKING POLICY ehdip_db.silver.ssn_mask;
ALTER TABLE ehdip_db.silver.omop_person MODIFY COLUMN birth_datetime SET MASKING POLICY ehdip_db.silver.dob_mask;

-- 4. Grant Select Privileges
GRANT SELECT ON ALL TABLES IN SCHEMA ehdip_db.silver TO ROLE ehdip_data_engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA ehdip_db.gold TO ROLE ehdip_data_engineer;

GRANT SELECT ON ALL TABLES IN SCHEMA ehdip_db.silver TO ROLE ehdip_data_scientist;

GRANT SELECT ON ALL TABLES IN SCHEMA ehdip_db.gold TO ROLE ehdip_clinical_analyst;
