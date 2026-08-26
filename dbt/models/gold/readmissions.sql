{{ config(
    materialized='table',
    tags=['gold', 'hedis']
) }}

WITH condition_events AS (
    SELECT
        person_source_value as patient_id,
        condition_concept_id,
        condition_start_date as admission_date
    FROM {{ source('silver', 'condition_occurrence') }}
    -- Assuming concept_id 32020 represents a hospital admission for simplicity
    WHERE condition_concept_id = 32020
),
ordered_admissions AS (
    SELECT
        patient_id,
        admission_date,
        LAG(admission_date) OVER (PARTITION BY patient_id ORDER BY admission_date) as prev_admission_date
    FROM condition_events
)
SELECT
    patient_id,
    admission_date,
    prev_admission_date,
    DATEDIFF(day, prev_admission_date, admission_date) as days_since_last_admission,
    CASE
        WHEN DATEDIFF(day, prev_admission_date, admission_date) <= 30 THEN 1
        ELSE 0
    END as is_30_day_readmission
FROM ordered_admissions
WHERE prev_admission_date IS NOT NULL
