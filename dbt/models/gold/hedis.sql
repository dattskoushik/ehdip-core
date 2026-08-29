-- dbt/models/gold/hedis.sql
-- HEDIS Quality Measures (e.g., Controlling High Blood Pressure)

{{ config(materialized='table') }}

WITH hypertension_patients AS (
    SELECT DISTINCT person_id
    FROM {{ source('silver', 'omop_condition_occurrence') }}
    WHERE condition_concept_id IN (316866, 31967) -- Mock SNOMED for Hypertension
),
blood_pressure_readings AS (
    SELECT
        person_id,
        measurement_date,
        value_as_number AS systolic_bp
    FROM {{ source('silver', 'omop_measurement') }} -- Assuming measurement table
    WHERE measurement_concept_id = 3004249 -- Systolic blood pressure
)

SELECT
    hp.person_id,
    MAX(bp.measurement_date) AS latest_bp_date,
    MAX_BY(bp.systolic_bp, bp.measurement_date) AS latest_systolic,
    CASE
        WHEN MAX_BY(bp.systolic_bp, bp.measurement_date) < 140 THEN TRUE
        ELSE FALSE
    END AS is_controlled
FROM hypertension_patients hp
LEFT JOIN blood_pressure_readings bp ON hp.person_id = bp.person_id
GROUP BY hp.person_id
