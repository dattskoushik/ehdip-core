{{ config(materialized='table') }}

WITH discharges AS (
    SELECT
        person_id,
        visit_occurrence_id,
        visit_end_date as discharge_date,
        visit_concept_id
    FROM {{ source('ehdip_silver', 'visit_occurrence') }}
    WHERE visit_concept_id = 9201 -- Inpatient Visit
),

readmissions AS (
    SELECT
        d1.person_id,
        d1.visit_occurrence_id as index_visit_id,
        d1.discharge_date as index_discharge_date,
        d2.visit_occurrence_id as readmission_visit_id,
        d2.visit_start_date as readmission_date,
        DATEDIFF('day', d1.discharge_date, d2.visit_start_date) as days_to_readmission
    FROM discharges d1
    JOIN {{ source('ehdip_silver', 'visit_occurrence') }} d2
        ON d1.person_id = d2.person_id
        AND d2.visit_start_date > d1.discharge_date
        AND DATEDIFF('day', d1.discharge_date, d2.visit_start_date) <= 30
        AND d2.visit_concept_id = 9201
)

SELECT
    person_id,
    COUNT(DISTINCT index_visit_id) as total_index_admissions,
    COUNT(DISTINCT readmission_visit_id) as total_30d_readmissions,
    CASE
        WHEN COUNT(DISTINCT index_visit_id) > 0 THEN
            CAST(COUNT(DISTINCT readmission_visit_id) AS FLOAT) / COUNT(DISTINCT index_visit_id)
        ELSE 0
    END as readmission_rate
FROM readmissions
GROUP BY person_id
