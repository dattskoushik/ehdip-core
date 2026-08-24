{{ config(
    materialized='table',
    tags=['gold', 'clinical_quality']
) }}

WITH index_admissions AS (
    SELECT
        person_id,
        condition_start_date AS index_date,
        condition_concept_id
    FROM {{ source('ehdip_silver_db', 'condition_occurrence') }}
    -- Assume specific concept IDs denote an inpatient admission
    WHERE condition_concept_id IN (9201, 262)
),

subsequent_admissions AS (
    SELECT
        person_id,
        condition_start_date AS readmission_date,
        condition_concept_id
    FROM {{ source('ehdip_silver_db', 'condition_occurrence') }}
    WHERE condition_concept_id IN (9201, 262)
)

SELECT
    i.person_id,
    i.index_date,
    s.readmission_date,
    DATEDIFF('day', i.index_date, s.readmission_date) AS days_to_readmission,
    CASE
        WHEN DATEDIFF('day', i.index_date, s.readmission_date) <= 30 THEN 1
        ELSE 0
    END AS is_30_day_readmission
FROM index_admissions i
JOIN subsequent_admissions s
  ON i.person_id = s.person_id
  AND s.readmission_date > i.index_date
  AND s.readmission_date <= DATEADD('day', 30, i.index_date)
