-- Role Based Access Control (RBAC) Setup
CREATE ROLE IF NOT EXISTS ehdip_data_engineer;
CREATE ROLE IF NOT EXISTS ehdip_data_analyst;

-- Grant Roles
GRANT ROLE ehdip_data_engineer TO USER dev_user;
GRANT ROLE ehdip_data_analyst TO USER analyst_user;

-- Grant Database Privileges
GRANT USAGE ON DATABASE ehdip_db TO ROLE ehdip_data_engineer;
GRANT USAGE ON DATABASE ehdip_db TO ROLE ehdip_data_analyst;

GRANT USAGE ON SCHEMA ehdip_db.silver TO ROLE ehdip_data_engineer;
GRANT USAGE ON SCHEMA ehdip_db.silver TO ROLE ehdip_data_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA ehdip_db.silver TO ROLE ehdip_data_engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA ehdip_db.silver TO ROLE ehdip_data_analyst;

-- Dynamic Data Masking for remaining sensitive fields (e.g. if SSN/MRN somehow bypasses or needs PII tagging)
CREATE OR REPLACE MASKING POLICY ehdip_ssn_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_DATA_ENGINEER') THEN val
    ELSE '***-**-****'
  END;

CREATE OR REPLACE MASKING POLICY ehdip_mrn_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_DATA_ENGINEER') THEN val
    ELSE 'REDACTED_MRN'
  END;

-- Apply Masking Policies to Silver/Gold Tables (Assuming these are native or manageable tables/views)
-- Example application on a view or table where these exist
-- ALTER TABLE ehdip_db.silver.patient_demographics MODIFY COLUMN ssn SET MASKING POLICY ehdip_ssn_mask;
-- ALTER TABLE ehdip_db.silver.patient_demographics MODIFY COLUMN mrn SET MASKING POLICY ehdip_mrn_mask;
