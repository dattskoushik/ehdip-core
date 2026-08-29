from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
# Note: For OpenLineage emission, MWAA/Airflow environment needs `apache-airflow-providers-openlineage` installed and configured.

default_args = {
    'owner': 'ehdip_data_eng',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Emr Serverless Application ID
EMR_SERVERLESS_APP_ID = '00fc123456789abc'
EXECUTION_ROLE_ARN = 'arn:aws:iam::123456789012:role/ehdip-emr-serverless-execution-role'

with DAG(
    'ehdip_daily_batch_pipeline',
    default_args=default_args,
    description='EHDIP E2E Pipeline: Ingest -> DeID -> OMOP -> DQ -> Gold',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['ehdip', 'production', 'healthcare']
) as dag:

    # 1. Batch Ingestion (X12)
    ingest_x12 = EmrServerlessStartJobOperator(
        task_id='ingest_x12_to_bronze',
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-scripts/src/ingestion/x12_batch_parser.py",
                "entryPointArguments": [
                    "--input-path", "s3://ehdip-landing/x12/",
                    "--iceberg-table", "ehdip_bronze.bronze_raw_payload"
                ],
                "sparkSubmitParameters": "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": "s3://ehdip-logs/emr-serverless/"}
            }
        },
    )

    # 2. De-Identification Engine
    deid_engine = EmrServerlessStartJobOperator(
        task_id='run_deid_engine',
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-scripts/src/security/deid_engine.py",
                "entryPointArguments": [
                    "--input-table", "ehdip_bronze.bronze_raw_payload",
                    "--output-table", "ehdip_silver.deid_payloads"
                ]
            }
        }
    )

    # 3. Silver Standardization (OMOP)
    omop_transformer = EmrServerlessStartJobOperator(
        task_id='transform_to_omop',
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-scripts/src/transformation/omop_silver_transformer.py",
                "entryPointArguments": [
                    "--input-table", "ehdip_silver.deid_payloads",
                    "--output-condition-table", "ehdip_silver.omop_condition_occurrence_raw"
                ]
            }
        }
    )

    # 4. Data Quality & DLQ Routing
    dq_check = EmrServerlessStartJobOperator(
        task_id='data_quality_and_dlq',
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-scripts/src/quality/dq_circuit_breaker.py",
                "entryPointArguments": [
                    "--input-table", "ehdip_silver.omop_condition_occurrence_raw",
                    "--valid-output-table", "ehdip_silver.omop_condition_occurrence_valid",
                    "--dlq-output-table", "ehdip_silver.omop_condition_occurrence_dlq"
                ]
            }
        }
    )

    # 5. Incremental Merge (CDC/Upsert)
    incremental_merge = EmrServerlessStartJobOperator(
        task_id='incremental_merge_silver',
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://ehdip-scripts/src/transformation/incremental_merge.py",
                "entryPointArguments": [
                    "--updates-table", "ehdip_silver.omop_condition_occurrence_valid",
                    "--target-iceberg-table", "ehdip_silver.omop_condition_occurrence",
                    "--primary-key", "condition_occurrence_id",
                    "--sort-key", "condition_start_date" # Assuming start date as proxy for recency in this mock
                ]
            }
        }
    )

    # 6. dbt Gold Marts Execution
    # Note: Using DbtCloudRunJobOperator as a proxy. In a fully localized setup,
    # a bash operator running `dbt run` on MWAA or an ECS task would be used.
    dbt_run_gold = DbtCloudRunJobOperator(
        task_id='dbt_run_gold_marts',
        dbt_cloud_conn_id='dbt_cloud_default',
        job_id=12345 # ID of the dbt Cloud job running models/gold/
    )

    # Pipeline Orchestration
    ingest_x12 >> deid_engine >> omop_transformer >> dq_check >> incremental_merge >> dbt_run_gold
