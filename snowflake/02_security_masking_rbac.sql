-- RBAC Roles
CREATE ROLE IF NOT EXISTS ehdip_analyst;
CREATE ROLE IF NOT EXISTS ehdip_data_scientist;

-- Dynamic Data Masking Policy
CREATE OR REPLACE MASKING POLICY ehdip_phi_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('EHDIP_DATA_SCIENTIST', 'SYSADMIN') THEN val
    ELSE '***MASKED***'
  END;

-- Apply Masking Policy (Example on a mock patient table if it existed in Snowflake)
-- ALTER TABLE ehdip_silver.patients MODIFY COLUMN ssn SET MASKING POLICY ehdip_phi_mask;

-- Grant access
GRANT USAGE ON DATABASE ehdip_db TO ROLE ehdip_analyst;
GRANT USAGE ON SCHEMA ehdip_db.ehdip_gold TO ROLE ehdip_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA ehdip_db.ehdip_gold TO ROLE ehdip_analyst;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA ehdip_db.ehdip_gold TO ROLE ehdip_analyst;
