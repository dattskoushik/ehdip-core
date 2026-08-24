-- dbt model: HEDIS Measures (Gold Mart)
{{ config(
    materialized='table',
    schema='gold'
) }}

WITH diabetic_patients AS (
    SELECT DISTINCT person_id
    FROM {{ source('ehdip_silver', 'condition_occurrence') }}
    WHERE condition_concept_id = 201820 -- Diabetes
),

hba1c_tests AS (
    SELECT
        person_id,
        measurement_date,
        value_as_number
    FROM {{ source('ehdip_silver', 'measurement') }}
    WHERE measurement_concept_id = 3004410 -- HbA1c
)

SELECT
    d.person_id,
    CASE WHEN h.person_id IS NOT NULL THEN 1 ELSE 0 END as had_hba1c_test,
    MAX(h.measurement_date) as last_test_date,
    MAX(h.value_as_number) as last_test_value
FROM diabetic_patients d
LEFT JOIN hba1c_tests h ON d.person_id = h.person_id
GROUP BY d.person_id, CASE WHEN h.person_id IS NOT NULL THEN 1 ELSE 0 END
