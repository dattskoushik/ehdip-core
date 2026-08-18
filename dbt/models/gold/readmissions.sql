{{ config(
    materialized='incremental',
    unique_key='patient_id'
) }}

WITH condition_events AS (
    SELECT
        person_id as patient_id,
        condition_start_date as admission_date,
        condition_concept_id
    FROM {{ source('ehdip_silver', 'condition_occurrence') }}
),

readmissions AS (
    SELECT
        patient_id,
        admission_date,
        LEAD(admission_date) OVER (PARTITION BY patient_id ORDER BY admission_date) as next_admission_date
    FROM condition_events
)

SELECT
    patient_id,
    COUNT(*) as total_admissions,
    SUM(CASE WHEN DATEDIFF('day', admission_date, next_admission_date) <= 30 THEN 1 ELSE 0 END) as thirty_day_readmissions
FROM readmissions
GROUP BY patient_id
