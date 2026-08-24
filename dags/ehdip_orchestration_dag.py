from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime, timedelta

# EHDIP Orchestration DAG for Batch Processing
default_args = {
    'owner': 'ehdip_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ehdip_end_to_end_pipeline',
    default_args=default_args,
    description='EHDIP Data Pipeline: Bronze -> Silver -> DQ -> Gold',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['ehdip', 'production'],
    # OpenLineage integration is typically configured at the Airflow cluster level
    # (e.g. MWAA environment variables for AIRFLOW__LINEAGE__BACKEND)
) as dag:

    # 1. Ingestion to Bronze (Batch X12 as an example)
    ingest_bronze = SparkSubmitOperator(
        task_id='ingest_to_bronze',
        application='src/ingestion/x12_batch_parser.py',
        application_args=['s3://ehdip-landing/x12/latest/', 'glue_catalog.ehdip_data_lake.bronze_raw_payloads'],
        conf={'spark.openlineage.namespace': 'ehdip-prod'}
    )

    # 2. De-identification
    deidentify_phi = SparkSubmitOperator(
        task_id='deidentify_phi',
        application='src/security/deid_engine.py',
        application_args=['glue_catalog.ehdip_data_lake.bronze_raw_payloads', 'glue_catalog.ehdip_data_lake.bronze_deid_payloads'],
        conf={'spark.openlineage.namespace': 'ehdip-prod'}
    )

    # 3. OMOP Transformation (Silver)
    transform_silver = SparkSubmitOperator(
        task_id='transform_silver_omop',
        application='src/transformation/omop_silver_transformer.py',
        application_args=['glue_catalog.ehdip_data_lake.bronze_deid_payloads', 'glue_catalog.ehdip_data_lake'],
        conf={'spark.openlineage.namespace': 'ehdip-prod'}
    )

    # 4. Data Quality & DLQ Routing
    data_quality_check = SparkSubmitOperator(
        task_id='data_quality_validation',
        application='src/quality/dq_circuit_breaker.py',
        application_args=[
            'glue_catalog.ehdip_data_lake.condition_occurrence',
            'glue_catalog.ehdip_data_lake.condition_occurrence_valid',
            'glue_catalog.ehdip_data_lake.condition_occurrence_dlq'
        ],
        conf={'spark.openlineage.namespace': 'ehdip-prod'}
    )

    # 5. Run dbt Gold Models (Using dbt Cloud as an example in a modern stack)
    # Alternatively, could be a BashOperator running dbt-core
    run_dbt_gold = DbtCloudRunJobOperator(
        task_id="run_dbt_gold_marts",
        job_id=12345, # Simulated Job ID
        check_interval=30,
        timeout=300
    )

    # Define Dependencies
    ingest_bronze >> deidentify_phi >> transform_silver >> data_quality_check >> run_dbt_gold
