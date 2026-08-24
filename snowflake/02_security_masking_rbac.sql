-- 1. Create RBAC Roles for EHDIP Data Platform
CREATE ROLE IF NOT EXISTS ehdip_data_engineer;
CREATE ROLE IF NOT EXISTS ehdip_data_scientist;
CREATE ROLE IF NOT EXISTS ehdip_clinical_analyst;

-- 2. Grant hierarchy
GRANT ROLE ehdip_clinical_analyst TO ROLE ehdip_data_scientist;
GRANT ROLE ehdip_data_scientist TO ROLE ehdip_data_engineer;
GRANT ROLE ehdip_data_engineer TO ROLE sysadmin;

-- 3. Create Dynamic Data Masking Policies (HIPAA Requirement)
-- Only Engineers and specific clinical roles see unmasked PII. Others see masked.
CREATE OR REPLACE MASKING POLICY ssn_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_DATA_ENGINEER') THEN val
    ELSE '***-**-****'
  END;

CREATE OR REPLACE MASKING POLICY mrn_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_DATA_ENGINEER', 'EHDIP_CLINICAL_ANALYST') THEN val
    ELSE '***-REDACTED-***'
  END;

CREATE OR REPLACE MASKING POLICY dob_mask AS (val date) RETURNS date ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_DATA_ENGINEER', 'EHDIP_CLINICAL_ANALYST') THEN val
    ELSE DATE_TRUNC('YEAR', val) -- Mask to just the year
  END;

-- 4. Apply Masking Policies to Gold Marts (Assume Gold tables exist or apply to views)
-- Example applying to a Gold dimension table
-- ALTER TABLE gold.dim_patient MODIFY COLUMN ssn SET MASKING POLICY ssn_mask;
-- ALTER TABLE gold.dim_patient MODIFY COLUMN mrn SET MASKING POLICY mrn_mask;
-- ALTER TABLE gold.dim_patient MODIFY COLUMN birth_date SET MASKING POLICY dob_mask;

-- 5. Grant Access to Gold layer
-- GRANT SELECT ON ALL TABLES IN SCHEMA gold TO ROLE ehdip_clinical_analyst;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA gold TO ROLE ehdip_clinical_analyst;
