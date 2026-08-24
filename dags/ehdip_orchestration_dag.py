from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default args with OpenLineage enabled inherently via Airflow configuration
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ehdip_end_to_end_orchestration',
    default_args=default_args,
    description='EHDIP Batch Pipeline: Raw -> Bronze -> Silver (OMOP) -> Gold (dbt)',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['ehdip', 'hipaa', 'production'],
) as dag:

    # 1. Ingestion (Batch X12)
    ingest_x12 = EmrServerlessStartJobOperator(
        task_id='ingest_x12_batch',
        application_id='app-xxxxx',
        execution_role_arn='arn:aws:iam::123456789012:role/EMRServerlessExecutionRole',
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-artifacts/scripts/x12_batch_parser.py',
                'entryPointArguments': ['s3://ehdip-raw-landing/x12/inbound/']
            }
        },
        name='ehdip_ingest_x12'
    )

    # 1b. Ingestion (Batch CDC)
    ingest_cdc = EmrServerlessStartJobOperator(
        task_id='ingest_cdc_batch',
        application_id='app-xxxxx',
        execution_role_arn='arn:aws:iam::123456789012:role/EMRServerlessExecutionRole',
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-artifacts/scripts/cdc_consumer.py',
                'entryPointArguments': ['s3://ehdip-raw-landing/cdc/inbound/']
            }
        },
        name='ehdip_ingest_cdc'
    )

    # 2. De-identification (FPE, Date Shift)
    deid_phi = EmrServerlessStartJobOperator(
        task_id='deidentify_phi',
        application_id='app-xxxxx',
        execution_role_arn='arn:aws:iam::123456789012:role/EMRServerlessExecutionRole',
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-artifacts/scripts/deid_engine.py'
            }
        },
        name='ehdip_deid'
    )

    # 3. Quality validation and DLQ routing
    data_quality_check = EmrServerlessStartJobOperator(
        task_id='data_quality_validation',
        application_id='app-xxxxx',
        execution_role_arn='arn:aws:iam::123456789012:role/EMRServerlessExecutionRole',
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-artifacts/scripts/dq_circuit_breaker.py'
            }
        },
        name='ehdip_dq'
    )

    # 4. Silver Transformation (OMOP Mapping & Upsert)
    silver_omop_transform = EmrServerlessStartJobOperator(
        task_id='silver_omop_transformation',
        application_id='app-xxxxx',
        execution_role_arn='arn:aws:iam::123456789012:role/EMRServerlessExecutionRole',
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-artifacts/scripts/omop_silver_transformer.py'
            }
        },
        name='ehdip_silver_omop'
    )

    # 5. Silver Incremental Merge
    silver_incremental_merge = EmrServerlessStartJobOperator(
        task_id='silver_incremental_merge',
        application_id='app-xxxxx',
        execution_role_arn='arn:aws:iam::123456789012:role/EMRServerlessExecutionRole',
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-artifacts/scripts/incremental_merge.py'
            }
        },
        name='ehdip_silver_merge'
    )

    # 6. dbt Gold Marts Execution (via Snowflake)
    run_dbt_gold_marts = DbtCloudRunJobOperator(
        task_id='run_dbt_gold_models',
        dbt_cloud_conn_id='dbt_cloud_default',
        job_id=12345
    )

    # DAG Dependencies
    [ingest_x12, ingest_cdc] >> deid_phi >> data_quality_check >> silver_omop_transform >> silver_incremental_merge >> run_dbt_gold_marts
