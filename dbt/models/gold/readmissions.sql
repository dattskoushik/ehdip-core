-- dbt/models/gold/readmissions.sql

{{ config(
    materialized='table',
    tags=['gold', 'hedis']
) }}

WITH condition_data AS (
    SELECT
        person_id,
        condition_start_date,
        condition_concept_id
    FROM {{ source('silver', 'condition_occurrence') }}
    WHERE condition_type_concept_id = 32020 -- Inpatient visits
),
readmissions AS (
    SELECT
        a.person_id,
        a.condition_start_date AS index_admission_date,
        b.condition_start_date AS readmission_date,
        DATEDIFF(day, a.condition_start_date, b.condition_start_date) as days_to_readmission
    FROM condition_data a
    JOIN condition_data b
        ON a.person_id = b.person_id
        AND b.condition_start_date > a.condition_start_date
        AND DATEDIFF(day, a.condition_start_date, b.condition_start_date) <= 30
)

SELECT
    person_id,
    COUNT(*) as thirty_day_readmission_count
FROM readmissions
GROUP BY person_id
