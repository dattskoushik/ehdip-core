-- Day 9: Snowflake Security, Dynamic Masking & dbt Gold Marts

-- 1. Create Roles (RBAC)
CREATE ROLE IF NOT EXISTS ehdip_data_scientist;
CREATE ROLE IF NOT EXISTS ehdip_data_analyst;
CREATE ROLE IF NOT EXISTS ehdip_compliance_officer;

-- 2. Create Dynamic Data Masking Policies for PHI
CREATE OR REPLACE MASKING POLICY ssn_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_COMPLIANCE_OFFICER') THEN val
    ELSE '***-**-****'
  END;

CREATE OR REPLACE MASKING POLICY mrn_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_COMPLIANCE_OFFICER', 'EHDIP_DATA_SCIENTIST') THEN val
    ELSE 'REDACTED_MRN'
  END;

CREATE OR REPLACE MASKING POLICY dob_mask AS (val date) RETURNS date ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_COMPLIANCE_OFFICER') THEN val
    ELSE DATE_TRUNC('YEAR', val) -- Mask to only show the year
  END;

-- 3. Apply Masking Policies to Tables (Assuming view or table exists)
-- E.g., ALTER TABLE gold_patient_profiles MODIFY COLUMN ssn SET MASKING POLICY ssn_mask;
-- ALTER TABLE gold_patient_profiles MODIFY COLUMN mrn SET MASKING POLICY mrn_mask;
-- ALTER TABLE gold_patient_profiles MODIFY COLUMN dob SET MASKING POLICY dob_mask;

-- 4. Grant Permissions
GRANT USAGE ON DATABASE ehdip TO ROLE ehdip_data_scientist;
GRANT USAGE ON SCHEMA ehdip.gold TO ROLE ehdip_data_scientist;
GRANT SELECT ON ALL TABLES IN SCHEMA ehdip.gold TO ROLE ehdip_data_scientist;

GRANT USAGE ON DATABASE ehdip TO ROLE ehdip_data_analyst;
GRANT USAGE ON SCHEMA ehdip.gold TO ROLE ehdip_data_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA ehdip.gold TO ROLE ehdip_data_analyst;
