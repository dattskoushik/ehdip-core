-- snowflake/02_security_masking_rbac.sql
USE ROLE ACCOUNTADMIN;

-- 1. Create RBAC Roles
CREATE ROLE IF NOT EXISTS ehdip_analyst_role;
CREATE ROLE IF NOT EXISTS ehdip_data_scientist_role;
CREATE ROLE IF NOT EXISTS ehdip_admin_role;

-- Grant permissions
GRANT USAGE ON DATABASE EHDIP_DB TO ROLE ehdip_analyst_role;
GRANT USAGE ON SCHEMA EHDIP_DB.GOLD TO ROLE ehdip_analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA EHDIP_DB.GOLD TO ROLE ehdip_analyst_role;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA EHDIP_DB.GOLD TO ROLE ehdip_analyst_role;

-- Admin gets everything
GRANT ROLE ehdip_analyst_role TO ROLE ehdip_admin_role;

-- 2. Dynamic Data Masking Policies
CREATE OR REPLACE MASKING POLICY ssn_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_ADMIN_ROLE') THEN val
    ELSE '***-**-****'
  END;

CREATE OR REPLACE MASKING POLICY mrn_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_ADMIN_ROLE') THEN val
    ELSE 'REDACTED'
  END;

CREATE OR REPLACE MASKING POLICY dob_mask AS (val date) RETURNS date ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_ADMIN_ROLE') THEN val
    ELSE DATE_TRUNC('YEAR', val) -- Only show the year for non-admins
  END;

-- Assuming a Gold patient table exists or will be created
-- ALTER TABLE EHDIP_DB.GOLD.patient_dim MODIFY COLUMN ssn SET MASKING POLICY ssn_mask;
-- ALTER TABLE EHDIP_DB.GOLD.patient_dim MODIFY COLUMN mrn SET MASKING POLICY mrn_mask;
-- ALTER TABLE EHDIP_DB.GOLD.patient_dim MODIFY COLUMN birth_date SET MASKING POLICY dob_mask;
