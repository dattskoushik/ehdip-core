# dags/ehdip_orchestration_dag.py

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from airflow.operators.bash import BashOperator
# In a real Airflow environment with OpenLineage, you'd set up the provider.
# from airflow.providers.openlineage.extractors import ...


# Default arguments for DAG
default_args = {
    'owner': 'ehdip_data_eng',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'ehdip_end_to_end_pipeline',
    default_args=default_args,
    description='EHDIP Medallion Architecture Orchestration',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['ehdip', 'production', 'iceberg', 'snowflake'],
) as dag:

    # 1. Batch Ingestion (X12 EDI) -> Bronze
    ingest_x12_bronze = EmrServerlessStartJobRunOperator(
        task_id='ingest_x12_to_bronze',
        application_id='app-12345xxxxxx',
        execution_role_arn='arn:aws:iam::123456789012:role/EMR_Serverless_Execution_Role',
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-scripts/ingestion/x12_batch_parser.py',
            }
        },
    )

    # 2. CDC Ingestion -> Bronze
    ingest_cdc_bronze = EmrServerlessStartJobRunOperator(
        task_id='ingest_cdc_to_bronze',
        application_id='app-12345xxxxxx',
        execution_role_arn='arn:aws:iam::123456789012:role/EMR_Serverless_Execution_Role',
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-scripts/ingestion/cdc_consumer.py',
            }
        },
    )

    # 3. De-Identification Engine (Bronze -> Silver)
    run_deid_engine = EmrServerlessStartJobRunOperator(
        task_id='run_phi_deidentification',
        application_id='app-12345xxxxxx',
        execution_role_arn='arn:aws:iam::123456789012:role/EMR_Serverless_Execution_Role',
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-scripts/security/deid_engine.py',
            }
        },
    )

    # 4. Data Quality & DLQ Routing
    run_dq_checks = EmrServerlessStartJobRunOperator(
        task_id='run_dq_circuit_breaker',
        application_id='app-12345xxxxxx',
        execution_role_arn='arn:aws:iam::123456789012:role/EMR_Serverless_Execution_Role',
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-scripts/quality/dq_circuit_breaker.py',
            }
        },
    )

    # 5. Silver OMOP Transformation
    transform_omop_silver = EmrServerlessStartJobRunOperator(
        task_id='transform_to_omop_silver',
        application_id='app-12345xxxxxx',
        execution_role_arn='arn:aws:iam::123456789012:role/EMR_Serverless_Execution_Role',
        job_driver={
            'sparkSubmit': {
                'entryPoint': 's3://ehdip-scripts/transformation/omop_silver_transformer.py',
            }
        },
    )


    # 6. Trigger dbt models for Gold Marts
    run_dbt_gold_marts = BashOperator(
        task_id='run_dbt_gold_marts',
        bash_command='dbt run --models gold.* --profiles-dir /usr/local/airflow/dbt',
        env={'OPENLINEAGE_URL': 'http://openlineage:5000', 'OPENLINEAGE_NAMESPACE': 'ehdip_prod'},
    )


    # Define Dependencies
    [ingest_x12_bronze, ingest_cdc_bronze] >> run_deid_engine >> run_dq_checks >> transform_omop_silver >> run_dbt_gold_marts
