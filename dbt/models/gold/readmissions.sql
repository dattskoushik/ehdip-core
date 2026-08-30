{{ config(materialized='table') }}

WITH index_admissions AS (
    SELECT
        person_id,
        condition_start_date AS index_date,
        condition_source_value
    FROM {{ source('ehdip_silver', 'dt_condition_occurrence') }}
    WHERE condition_source_value = 'INDEX_CONDITION' -- Mock condition
),
subsequent_admissions AS (
    SELECT
        person_id,
        condition_start_date AS subsequent_date
    FROM {{ source('ehdip_silver', 'dt_condition_occurrence') }}
)

SELECT
    i.person_id,
    i.index_date,
    COUNT(s.subsequent_date) AS readmission_count_30d
FROM index_admissions i
LEFT JOIN subsequent_admissions s
    ON i.person_id = s.person_id
    AND s.subsequent_date > i.index_date
    AND DATEDIFF('day', i.index_date, s.subsequent_date) <= 30
GROUP BY i.person_id, i.index_date
