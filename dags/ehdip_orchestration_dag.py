from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# EMR Serverless App ID would be dynamically retrieved or set via Airflow Variables in a real env
EMR_SERVERLESS_APP_ID = "00f9xxxxx"
EXECUTION_ROLE_ARN = "arn:aws:iam::123456789012:role/ehdip-emr-serverless-role"

with DAG(
    'ehdip_end_to_end_orchestration',
    default_args=default_args,
    description='EHDIP Batch Pipeline: Raw -> Bronze -> Silver (De-id & OMOP) -> DQ -> Gold (dbt)',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ehdip', 'production'],
) as dag:

    start_pipeline = DummyOperator(task_id='start_pipeline')

    # Step 1: Batch Ingestion (X12 and CDC) -> Bronze
    ingest_to_bronze = EmrServerlessStartJobOperator(
        task_id='ingest_to_bronze',
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-code/src/ingestion/x12_batch_parser.py",
                "entryPointArguments": ["s3://ehdip-raw/x12/inbound/"]
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": "s3://ehdip-logs/emr-serverless/"}
            }
        },
        name="ehdip_ingest_bronze"
    )

    # Step 2: PHI De-identification & OMOP Transformation -> Silver
    transform_to_silver = EmrServerlessStartJobOperator(
        task_id='transform_to_silver',
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-code/src/transformation/omop_silver_transformer.py",
                # The deid engine is imported and utilized within the transformer or as a preceding step
            }
        },
        name="ehdip_transform_silver"
    )

    # Step 3: Data Quality Circuit Breaker
    run_dq_checks = EmrServerlessStartJobOperator(
        task_id='run_dq_checks',
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-code/src/quality/dq_circuit_breaker.py",
            }
        },
        name="ehdip_dq_checks"
    )

    # Step 4: Run dbt Models (Gold Layer in Snowflake)
    # Using dbt Cloud operator for demonstration
    run_dbt_gold = DbtCloudRunJobOperator(
        task_id='run_dbt_gold_marts',
        dbt_cloud_conn_id='dbt_cloud_default',
        job_id=12345, # ID of the Gold Marts job in dbt Cloud
        check_interval=60,
        timeout=3600
    )

    end_pipeline = DummyOperator(task_id='end_pipeline')

    # Define DAG Dependencies
    start_pipeline >> ingest_to_bronze >> transform_to_silver >> run_dq_checks >> run_dbt_gold >> end_pipeline

    # OpenLineage metadata emission is typically handled automatically via Spark/Airflow integrations
    # provided the cluster/environment is configured with the OpenLineage spark listener.
