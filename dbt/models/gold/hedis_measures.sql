-- dbt/models/gold/hedis_measures.sql

{{ config(
    materialized='table',
    tags=['gold', 'hedis']
) }}

WITH patients AS (
    SELECT
        person_id,
        year_of_birth,
        gender_source_value
    FROM {{ source('silver', 'omop_person') }}
),

conditions AS (
    SELECT
        person_id,
        condition_concept_id,
        condition_start_date
    FROM {{ source('silver', 'omop_condition_occurrence') }}
)

SELECT
    p.person_id,
    p.year_of_birth,
    p.gender_source_value,
    -- Simple HEDIS measure proxy: Diabetes condition count
    COUNT(c.condition_concept_id) FILTER (WHERE c.condition_concept_id = 201820) as diabetes_condition_count,
    CURRENT_TIMESTAMP() as calculated_at
FROM patients p
LEFT JOIN conditions c ON p.person_id = c.person_id
GROUP BY
    p.person_id,
    p.year_of_birth,
    p.gender_source_value
