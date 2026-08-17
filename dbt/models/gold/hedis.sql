{{ config(
    materialized='view',
    schema='gold'
) }}

WITH condition_data AS (
    SELECT
        person_id,
        condition_concept_id,
        condition_start_date
    FROM {{ source('ehdip_silver', 'condition_occurrence') }}
),

-- Mock HEDIS Measure: Patients with a specific condition (e.g., Diabetes 201820) in the measurement year
hedis_diabetes_denominator AS (
    SELECT DISTINCT
        person_id
    FROM condition_data
    WHERE condition_concept_id = 201820
      AND YEAR(condition_start_date) = YEAR(CURRENT_DATE) - 1
)

SELECT
    person_id,
    TRUE AS in_diabetes_denominator
FROM hedis_diabetes_denominator
