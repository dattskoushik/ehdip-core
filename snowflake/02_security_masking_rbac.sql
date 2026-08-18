-- RBAC Role Definitions
CREATE OR REPLACE ROLE ehdip_data_scientist;
CREATE OR REPLACE ROLE ehdip_clinical_analyst;

-- Dynamic Data Masking Policies (HIPAA Compliance)
CREATE OR REPLACE MASKING POLICY ehdip.security.mask_ssn AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_CLINICAL_ANALYST', 'ACCOUNTADMIN') THEN val
    ELSE '***-**-****'
  END;

CREATE OR REPLACE MASKING POLICY ehdip.security.mask_mrn AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_CLINICAL_ANALYST', 'ACCOUNTADMIN') THEN val
    ELSE 'REDACTED_MRN'
  END;

-- Apply masking policies to Silver OMOP tables
-- (Assuming these tables were mounted via Iceberg integration)
ALTER TABLE ehdip.silver.omop_person
MODIFY COLUMN person_source_value SET MASKING POLICY ehdip.security.mask_mrn;

-- Grant access
GRANT USAGE ON DATABASE ehdip TO ROLE ehdip_data_scientist;
GRANT USAGE ON SCHEMA ehdip.silver TO ROLE ehdip_data_scientist;
GRANT SELECT ON TABLE ehdip.silver.omop_person TO ROLE ehdip_data_scientist;
