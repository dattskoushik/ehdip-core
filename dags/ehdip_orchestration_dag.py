from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable

# EMR Serverless App ID and Role ARN
emr_serverless_app_id = Variable.get("EMR_SERVERLESS_APP_ID", default_var="mock-app-id")
emr_serverless_role_arn = Variable.get("EMR_SERVERLESS_ROLE_ARN", default_var="arn:aws:iam::123:role/mock-role")
s3_bucket = "s3://ehdip-artifacts"

default_args = {
    'owner': 'ehdip_data_eng',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ehdip_orchestration_dag',
    default_args=default_args,
    description='EHDIP End-to-End Orchestration',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ehdip', 'production', 'openlineage'],
) as dag:

    # 1. Batch Ingestion (X12)
    ingest_x12 = EmrServerlessStartJobOperator(
        task_id='ingest_x12',
        application_id=emr_serverless_app_id,
        execution_role_arn=emr_serverless_role_arn,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"{s3_bucket}/scripts/x12_batch_parser.py",
                "entryPointArguments": [
                    "--input_path", "s3://ehdip-landing/x12/",
                    "--output_table", "ehdip_bronze_db.raw_payloads"
                ]
            }
        },
        name="ingest_x12_job"
    )

    # 2. De-Identification
    deid_job = EmrServerlessStartJobOperator(
        task_id='deid_job',
        application_id=emr_serverless_app_id,
        execution_role_arn=emr_serverless_role_arn,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"{s3_bucket}/scripts/deid_engine.py",
                "entryPointArguments": [
                    "--input_table", "ehdip_bronze_db.raw_payloads",
                    "--output_table", "ehdip_bronze_db.deid_payloads"
                ]
            }
        },
        name="deid_job"
    )

    # 3. OMOP Silver Transformation
    omop_transform = EmrServerlessStartJobOperator(
        task_id='omop_transform',
        application_id=emr_serverless_app_id,
        execution_role_arn=emr_serverless_role_arn,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"{s3_bucket}/scripts/omop_silver_transformer.py",
                "entryPointArguments": [
                    "--input_table", "ehdip_bronze_db.deid_payloads",
                    "--db_prefix", "ehdip_silver_db"
                ]
            }
        },
        name="omop_transform_job"
    )

    # 4. Data Quality Checks & DLQ Routing
    dq_checks = EmrServerlessStartJobOperator(
        task_id='dq_checks',
        application_id=emr_serverless_app_id,
        execution_role_arn=emr_serverless_role_arn,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"{s3_bucket}/scripts/dq_circuit_breaker.py",
                "entryPointArguments": [
                    "--input_table", "ehdip_silver_db.condition_occurrence_staging",
                    "--valid_output", "ehdip_silver_db.condition_occurrence",
                    "--dlq_output", "ehdip_silver_db.dlq_condition_occurrence"
                ]
            }
        },
        name="dq_checks_job"
    )

    # 5. Trigger Snowflake dbt runs (via SnowflakeOperator for demo, typically bash/dbt operator)
    run_dbt_models = SnowflakeOperator(
        task_id='trigger_gold_refresh',
        snowflake_conn_id='snowflake_default',
        sql="""
        -- In reality this would be calling dbt cloud or running dbt CLI
        -- Simulating refreshing dynamic tables manually if they weren't auto
        ALTER DYNAMIC TABLE ehdip_gold.dt_readmissions_base REFRESH;
        """
    )

    # Define dependencies
    ingest_x12 >> deid_job >> omop_transform >> dq_checks >> run_dbt_models
