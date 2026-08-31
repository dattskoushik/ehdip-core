{{ config(
    materialized='table',
    tags=['gold', 'clinical_metrics']
) }}

WITH condition_occurrences AS (
    -- Reference to the silver layer OMOP table
    SELECT
        condition_occurrence_id,
        person_id,
        condition_start_date
    FROM {{ source('ehdip_silver', 'condition_occurrence') }}
),

readmission_calc AS (
    SELECT
        a.person_id,
        a.condition_occurrence_id AS index_admission_id,
        a.condition_start_date AS index_date,
        b.condition_occurrence_id AS readmission_id,
        b.condition_start_date AS readmission_date,
        DATEDIFF(day, a.condition_start_date, b.condition_start_date) as days_to_readmit
    FROM condition_occurrences a
    JOIN condition_occurrences b
      ON a.person_id = b.person_id
      AND a.condition_occurrence_id != b.condition_occurrence_id
      AND b.condition_start_date > a.condition_start_date
      AND b.condition_start_date <= DATEADD(day, 30, a.condition_start_date)
)

SELECT
    person_id,
    COUNT(DISTINCT index_admission_id) as total_index_admissions,
    COUNT(DISTINCT readmission_id) as total_30_day_readmissions,
    CASE
        WHEN COUNT(DISTINCT index_admission_id) > 0
        THEN CAST(COUNT(DISTINCT readmission_id) AS FLOAT) / COUNT(DISTINCT index_admission_id)
        ELSE 0
    END as readmission_rate
FROM readmission_calc
GROUP BY person_id
