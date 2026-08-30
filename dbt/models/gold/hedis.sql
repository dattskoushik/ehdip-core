{{ config(materialized='table') }}

WITH conditions AS (
    SELECT
        person_id,
        condition_start_date,
        condition_source_value
    FROM {{ source('ehdip_silver', 'dt_condition_occurrence') }}
)

SELECT
    person_id,
    COUNT(CASE WHEN condition_source_value = 'DIABETES_COMPLICATION' THEN 1 END) AS diabetes_complications_count,
    COUNT(CASE WHEN condition_source_value = 'HYPERTENSION' THEN 1 END) AS hypertension_count
FROM conditions
WHERE YEAR(condition_start_date) = YEAR(CURRENT_DATE()) - 1
GROUP BY person_id
