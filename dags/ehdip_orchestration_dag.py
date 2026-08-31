import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

default_args = {
    'owner': 'ehdip_data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# OpenLineage configuration is typically managed via Airflow's core configurations
# or via specific connection setups in MWAA. By utilizing standard operators,
# Airflow will emit metadata automatically to the configured OpenLineage backend.

with DAG(
    'ehdip_end_to_end_pipeline',
    default_args=default_args,
    description='EHDIP Pipeline: Batch -> De-id -> Silver OMOP -> DQ -> DLQ -> Gold dbt',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ehdip', 'production', 'medallion', 'openlineage'],
) as dag:

    # 1. Ingest (Batch X12)
    ingest_batch = EmrServerlessStartJobOperator(
        task_id='ingest_x12_batch',
        application_id=os.environ.get('EMR_SERVERLESS_APP_ID'),
        execution_role_arn=os.environ.get('EMR_SERVERLESS_ROLE_ARN'),
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-scripts/ingestion/x12_batch_parser.py',
                'entryPointArguments': [
                    '--input-path', 's3://ehdip-raw-landing/x12/',
                    '--table', 'raw_payloads'
                ]
            }
        },
    )

    # 2. De-Identify (FPE and Date-Shifting)
    deidentify = EmrServerlessStartJobOperator(
        task_id='deidentify_phi',
        application_id=os.environ.get('EMR_SERVERLESS_APP_ID'),
        execution_role_arn=os.environ.get('EMR_SERVERLESS_ROLE_ARN'),
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-scripts/security/deid_engine.py',
                'entryPointArguments': [
                    '--input-table', 'raw_payloads',
                    '--output-table', 'deid_payloads'
                ]
            }
        },
    )

    # 3. Transform to Silver OMOP v5.4
    silver_omop = EmrServerlessStartJobOperator(
        task_id='transform_silver_omop',
        application_id=os.environ.get('EMR_SERVERLESS_APP_ID'),
        execution_role_arn=os.environ.get('EMR_SERVERLESS_ROLE_ARN'),
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-scripts/transformation/omop_silver_transformer.py',
                'entryPointArguments': [
                    '--input-table', 'deid_payloads',
                    '--silver-database', 'ehdip_silver_db',
                    '--output-table', 'condition_occurrence_raw'
                ]
            }
        },
    )

    # 4. Data Quality & DLQ Routing
    data_quality = EmrServerlessStartJobOperator(
        task_id='data_quality_and_dlq',
        application_id=os.environ.get('EMR_SERVERLESS_APP_ID'),
        execution_role_arn=os.environ.get('EMR_SERVERLESS_ROLE_ARN'),
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-scripts/quality/dq_circuit_breaker.py',
                'entryPointArguments': [
                    '--input-table', 'condition_occurrence_raw', # Assumed intermediate
                    '--valid-table', 'condition_occurrence_valid',
                    '--dlq-table', 'condition_occurrence_dlq'
                ]
            }
        },
    )

    # 5. Incremental Merge (Upsert)
    incremental_merge = EmrServerlessStartJobOperator(
        task_id='incremental_merge_silver',
        application_id=os.environ.get('EMR_SERVERLESS_APP_ID'),
        execution_role_arn=os.environ.get('EMR_SERVERLESS_ROLE_ARN'),
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-scripts/transformation/incremental_merge.py',
                'entryPointArguments': [
                    '--source-table', 'condition_occurrence_valid',
                    '--target-table', 'condition_occurrence'
                ]
            }
        },
    )

    # 6. Build Gold dbt Marts (Snowflake)
    # Using dbt Cloud runner as typical in modern data stacks
    build_gold_marts = DbtCloudRunJobOperator(
        task_id='build_gold_dbt_marts',
        dbt_cloud_conn_id='dbt_cloud_default',
        job_id=12345 # ID configured in dbt Cloud for ehdip-gold-job
    )

    # Define Dependencies
    ingest_batch >> deidentify >> silver_omop >> data_quality >> incremental_merge >> build_gold_marts
