{{ config(
    materialized='table',
    schema='gold'
) }}

WITH encounter_data AS (
    SELECT
        person_id,
        encounter_id,
        encounter_start_date,
        encounter_end_date,
        discharge_to_concept_id
    FROM {{ source('ehdip_silver', 'encounter') }}
),

readmissions AS (
    SELECT
        e1.person_id,
        e1.encounter_id AS index_encounter_id,
        e1.encounter_end_date AS index_discharge_date,
        e2.encounter_id AS readmission_encounter_id,
        e2.encounter_start_date AS readmission_date,
        DATEDIFF(day, e1.encounter_end_date, e2.encounter_start_date) AS days_to_readmission
    FROM encounter_data e1
    JOIN encounter_data e2
        ON e1.person_id = e2.person_id
        AND e1.encounter_id != e2.encounter_id
        AND e2.encounter_start_date > e1.encounter_end_date
        AND DATEDIFF(day, e1.encounter_end_date, e2.encounter_start_date) <= 30
)

SELECT * FROM readmissions
