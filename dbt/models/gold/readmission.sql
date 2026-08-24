-- dbt model: 30-day readmissions (Gold Mart)
{{ config(
    materialized='table',
    schema='gold'
) }}

WITH index_admissions AS (
    SELECT
        person_id,
        condition_start_date as admission_date,
        condition_end_date as discharge_date
    FROM {{ source('ehdip_silver', 'condition_occurrence') }}
    WHERE condition_concept_id = 9201 -- Inpatient Visit
),

subsequent_admissions AS (
    SELECT
        person_id,
        condition_start_date as readmission_date
    FROM {{ source('ehdip_silver', 'condition_occurrence') }}
    WHERE condition_concept_id = 9201
)

SELECT
    i.person_id,
    i.admission_date,
    i.discharge_date,
    s.readmission_date,
    CASE
        WHEN s.readmission_date IS NOT NULL THEN 1
        ELSE 0
    END as is_readmitted_30_days
FROM index_admissions i
LEFT JOIN subsequent_admissions s
    ON i.person_id = s.person_id
    AND s.readmission_date > i.discharge_date
    AND s.readmission_date <= i.discharge_date + INTERVAL '30 days'
