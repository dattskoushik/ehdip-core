from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.dates import days_ago

# Default args with OpenLineage enabled via environment or lineage provider config
default_args = {
    'owner': 'ehdip_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'ehdip_daily_batch_pipeline',
    default_args=default_args,
    description='EHDIP E2E Pipeline: Ingest -> DeID -> Silver OMOP -> DQ -> Gold',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['ehdip', 'core', 'hipaa', 'openlineage'],
) as dag:

    # 1. Ingest X12 / CDC (Bronze)
    ingest_bronze = EmrServerlessStartJobOperator(
        task_id='ingest_to_bronze',
        application_id='{{ var.value.emr_serverless_app_id }}',
        execution_role_arn='{{ var.value.emr_serverless_role_arn }}',
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-scripts/ingestion/x12_batch_parser.py",
                "entryPointArguments": ["s3://ehdip-landing/x12/", "ehdip_bronze_db.raw_payloads"]
            }
        },
        name="ingest_bronze"
    )

    # 2. De-Identify & FPE
    deidentify_phi = EmrServerlessStartJobOperator(
        task_id='deidentify_phi_data',
        application_id='{{ var.value.emr_serverless_app_id }}',
        execution_role_arn='{{ var.value.emr_serverless_role_arn }}',
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-scripts/security/deid_engine.py",
                "entryPointArguments": ["ehdip_bronze_db.raw_payloads", "ehdip_silver_db.deid_payloads", "clinical_notes"]
            }
        },
        name="deid_phi"
    )

    # 3. Transform to Silver (OMOP + Splink Linkage)
    transform_silver_omop = EmrServerlessStartJobOperator(
        task_id='transform_silver_omop',
        application_id='{{ var.value.emr_serverless_app_id }}',
        execution_role_arn='{{ var.value.emr_serverless_role_arn }}',
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-scripts/transformation/omop_silver_transformer.py",
                "entryPointArguments": ["ehdip_silver_db.deid_payloads", "ehdip_silver_db.concept_vocab", "ehdip_silver_db.condition_occurrence_raw", "ehdip_silver_db.drug_exposure_raw"]
            }
        },
        name="transform_silver"
    )

    # 4. Data Quality & DLQ
    data_quality_check = EmrServerlessStartJobOperator(
        task_id='data_quality_and_dlq',
        application_id='{{ var.value.emr_serverless_app_id }}',
        execution_role_arn='{{ var.value.emr_serverless_role_arn }}',
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-scripts/quality/dq_circuit_breaker.py",
                "entryPointArguments": ["ehdip_silver_db.condition_occurrence_raw", "ehdip_silver_db.condition_occurrence", "ehdip_bronze_db.dlq"]
            }
        },
        name="dq_check"
    )

    # 5. Incremental Merge (Silver Upsert)
    incremental_merge = EmrServerlessStartJobOperator(
        task_id='incremental_silver_merge',
        application_id='{{ var.value.emr_serverless_app_id }}',
        execution_role_arn='{{ var.value.emr_serverless_role_arn }}',
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-scripts/transformation/incremental_merge.py",
                "entryPointArguments": ["ehdip_silver_db.condition_occurrence", "ehdip_silver_db.condition_occurrence_final", "condition_occurrence_id"]
            }
        },
        name="silver_merge"
    )

    # 6. Trigger dbt Cloud Job for Gold Marts (Snowflake Dynamic Tables handle most, dbt for complex models)
    dbt_gold_marts = DbtCloudRunJobOperator(
        task_id='run_dbt_gold_models',
        dbt_cloud_conn_id='dbt_cloud_default',
        job_id=12345, # Example Job ID
        check_interval=60,
        timeout=3600
    )

    # Pipeline Dependencies
    ingest_bronze >> deidentify_phi >> transform_silver_omop >> data_quality_check >> incremental_merge >> dbt_gold_marts
