-- Day 9: Snowflake Security, Dynamic Data Masking, and RBAC Roles

USE ROLE ACCOUNTADMIN;
USE DATABASE EHDIP_PROD;
CREATE SCHEMA IF NOT EXISTS SECURITY;

-- 1. Create RBAC Roles
CREATE ROLE IF NOT EXISTS ehdip_data_engineer;
CREATE ROLE IF NOT EXISTS ehdip_analyst;
CREATE ROLE IF NOT EXISTS ehdip_phi_viewer;

-- Grant usage on database and schemas
GRANT USAGE ON DATABASE EHDIP_PROD TO ROLE ehdip_data_engineer;
GRANT USAGE ON SCHEMA EHDIP_PROD.SILVER TO ROLE ehdip_data_engineer;
GRANT USAGE ON SCHEMA EHDIP_PROD.GOLD TO ROLE ehdip_data_engineer;

GRANT USAGE ON DATABASE EHDIP_PROD TO ROLE ehdip_analyst;
GRANT USAGE ON SCHEMA EHDIP_PROD.GOLD TO ROLE ehdip_analyst;

GRANT USAGE ON DATABASE EHDIP_PROD TO ROLE ehdip_phi_viewer;
GRANT USAGE ON SCHEMA EHDIP_PROD.SILVER TO ROLE ehdip_phi_viewer;
GRANT USAGE ON SCHEMA EHDIP_PROD.GOLD TO ROLE ehdip_phi_viewer;

-- 2. Create Dynamic Data Masking Policies
-- Masking Policy for SSN/MRN
CREATE OR REPLACE MASKING POLICY SECURITY.mask_ssn_mrn AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'EHDIP_PHI_VIEWER') THEN val
    ELSE '***-**-****'
  END;

-- Masking Policy for Date of Birth (Show only year for non-PHI viewers)
CREATE OR REPLACE MASKING POLICY SECURITY.mask_dob AS (val date) RETURNS date ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'EHDIP_PHI_VIEWER') THEN val
    ELSE DATE_TRUNC('YEAR', val)
  END;

-- 3. Apply Masking Policies to Tables
-- Assuming a demographic table exists in Silver
-- ALTER TABLE SILVER.patient_demographics MODIFY COLUMN ssn SET MASKING POLICY SECURITY.mask_ssn_mrn;
-- ALTER TABLE SILVER.patient_demographics MODIFY COLUMN dob SET MASKING POLICY SECURITY.mask_dob;

-- 4. Grant Select Permissions
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA EHDIP_PROD.SILVER TO ROLE ehdip_data_engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA EHDIP_PROD.GOLD TO ROLE ehdip_data_engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA EHDIP_PROD.GOLD TO ROLE ehdip_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA EHDIP_PROD.SILVER TO ROLE ehdip_phi_viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA EHDIP_PROD.GOLD TO ROLE ehdip_phi_viewer;
