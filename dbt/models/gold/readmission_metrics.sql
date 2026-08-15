-- dbt/models/gold/readmission_metrics.sql

{{ config(
    materialized='table',
    tags=['gold', 'clinical']
) }}

WITH visits AS (
    SELECT
        person_id,
        visit_occurrence_id,
        visit_start_date,
        visit_end_date,
        LAG(visit_end_date) OVER (PARTITION BY person_id ORDER BY visit_start_date) as prev_discharge_date
    FROM {{ source('silver', 'omop_visit_occurrence') }}
)

SELECT
    person_id,
    COUNT(visit_occurrence_id) as total_visits,
    SUM(CASE WHEN DATEDIFF('day', prev_discharge_date, visit_start_date) <= 30 THEN 1 ELSE 0 END) as readmissions_within_30_days,
    CURRENT_TIMESTAMP() as calculated_at
FROM visits
GROUP BY person_id
