from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# EHDIP Orchestration DAG for AWS MWAA
# Assumes EMR Serverless for PySpark and dbt Cloud for Gold Marts

default_args = {
    'owner': 'ehdip_data_eng',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ehdip_medallion_pipeline_daily',
    default_args=default_args,
    description='E2E EHDIP Pipeline: Bronze -> Silver OMOP -> Gold dbt',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ehdip', 'medallion', 'omop'],
) as dag:

    start = EmptyOperator(task_id='start')

    # 1. Batch Ingestion (X12) -> Bronze
    ingest_x12_bronze = EmrServerlessStartJobOperator(
        task_id='ingest_x12_bronze',
        application_id='app-12345',
        execution_role_arn='arn:aws:iam::123456789012:role/EMRServerlessRole',
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-artifacts/scripts/x12_batch_parser.py",
            }
        },
    )

    # 2. De-ID & Transformation -> Silver OMOP
    transform_silver_omop = EmrServerlessStartJobOperator(
        task_id='transform_silver_omop',
        application_id='app-12345',
        execution_role_arn='arn:aws:iam::123456789012:role/EMRServerlessRole',
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-artifacts/scripts/omop_silver_transformer.py",
            }
        },
    )

    # 3. Incremental Upsert (CDC Merge) -> Silver OMOP
    incremental_upsert_silver = EmrServerlessStartJobOperator(
        task_id='incremental_upsert_silver',
        application_id='app-12345',
        execution_role_arn='arn:aws:iam::123456789012:role/EMRServerlessRole',
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-artifacts/scripts/incremental_merge.py",
            }
        },
    )

    # 4. Data Quality Check
    dq_circuit_breaker = EmrServerlessStartJobOperator(
        task_id='dq_circuit_breaker',
        application_id='app-12345',
        execution_role_arn='arn:aws:iam::123456789012:role/EMRServerlessRole',
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-artifacts/scripts/dq_circuit_breaker.py",
            }
        },
    )

    # 5. Run dbt Gold Models (Snowflake)
    run_dbt_gold_marts = DbtCloudRunJobOperator(
        task_id='run_dbt_gold_marts',
        dbt_cloud_conn_id='dbt_cloud_default',
        job_id=98765,
    )

    end = EmptyOperator(task_id='end')

    # Define Dependencies
    start >> ingest_x12_bronze >> transform_silver_omop >> incremental_upsert_silver >> dq_circuit_breaker >> run_dbt_gold_marts >> end
