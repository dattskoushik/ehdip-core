-- dbt/models/gold/readmissions.sql
-- 30-day Readmission Metric

{{ config(materialized='table') }}

WITH discharges AS (
    SELECT
        person_id,
        condition_occurrence_id AS discharge_encounter_id,
        condition_end_date AS discharge_date
    FROM {{ source('silver', 'omop_condition_occurrence') }}
    WHERE condition_end_date IS NOT NULL
      AND condition_type_concept_id = 32020 -- EHR encounter diagnosis
),
admissions AS (
    SELECT
        person_id,
        condition_occurrence_id AS admission_encounter_id,
        condition_start_date AS admission_date
    FROM {{ source('silver', 'omop_condition_occurrence') }}
    WHERE condition_type_concept_id = 32020
)

SELECT
    d.person_id,
    d.discharge_encounter_id,
    d.discharge_date,
    a.admission_encounter_id,
    a.admission_date,
    DATEDIFF('day', d.discharge_date, a.admission_date) AS days_to_readmission
FROM discharges d
JOIN admissions a
  ON d.person_id = a.person_id
 AND a.admission_date > d.discharge_date
 AND a.admission_date <= DATEADD('day', 30, d.discharge_date)
