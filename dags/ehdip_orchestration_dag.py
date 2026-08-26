import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# Using generic BashOperator to simulate dbt run in the DAG or using dbt-airflow
from airflow.operators.bash import BashOperator

# Default args with retry logic for idempotency
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Emr Serverless app id (usually provided via Airflow Variable or hardcoded in infra)
EMR_SERVERLESS_APP_ID = os.environ.get("EMR_SERVERLESS_APP_ID", "default_app_id")
EXECUTION_ROLE_ARN = os.environ.get("EMR_SERVERLESS_EXEC_ROLE_ARN", "default_role_arn")

def get_spark_submit_config(entry_point, arguments):
    # Including OpenLineage packages and spark configurations
    openlineage_packages = "io.openlineage:openlineage-spark_2.12:1.2.0"
    base_packages = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,com.amazon.deequ:deequ-2.0.7-spark-3.4_2.12"

    spark_submit_params = (
        f"--conf spark.jars.packages={base_packages},{openlineage_packages} "
        "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
        "--conf spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog "
        "--conf spark.sql.catalog.glue.type=glue "
        "--conf spark.sql.catalog.glue.warehouse=s3://ehdip-bronze-data-us-east-1/warehouse "
        "--conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener "
        "--conf spark.openlineage.transport.type=http "
        "--conf spark.openlineage.transport.url=http://openlineage-marquez:5000 "
        "--conf spark.openlineage.namespace=ehdip_production"
    )

    return {
        "sparkSubmit": {
            "entryPoint": entry_point,
            "entryPointArguments": arguments,
            "sparkSubmitParameters": spark_submit_params
        }
    }

with DAG(
    'ehdip_end_to_end_pipeline',
    default_args=default_args,
    description='EHDIP Batch, De-id, DQ, Transform, and Gold Pipeline with OpenLineage',
    schedule_interval='@daily',
    catchup=False,
    tags=['ehdip', 'production', 'healthcare']
) as dag:

    # 1. Batch Ingestion
    ingest_x12_batch = EmrServerlessStartJobOperator(
        task_id="ingest_x12_batch",
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver=get_spark_submit_config(
            "s3://ehdip-scripts/src/ingestion/x12_batch_parser.py",
            ["--input-path", "s3://ehdip-bronze-data-us-east-1/raw_x12/", "--bronze-table", "glue.bronze_db.raw_payloads"]
        ),
        name="ingest_x12_batch"
    )

    # 2. De-Identification Engine
    run_deid_engine = EmrServerlessStartJobOperator(
        task_id="run_deid_engine",
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver=get_spark_submit_config(
            "s3://ehdip-scripts/src/security/deid_engine.py",
            ["--input-table", "glue.bronze_db.raw_payloads", "--output-table", "glue.silver_db.deidentified_payloads"]
        ),
        name="run_deid_engine"
    )

    # 3. Silver Transformation (OMOP)
    run_omop_transformer = EmrServerlessStartJobOperator(
        task_id="run_omop_transformer",
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver=get_spark_submit_config(
            "s3://ehdip-scripts/src/transformation/omop_silver_transformer.py",
            ["--input-table", "glue.silver_db.deidentified_payloads", "--silver-db", "glue.silver_db"]
        ),
        name="run_omop_transformer"
    )

    # 4. Data Quality & DLQ Routing
    run_dq_circuit_breaker = EmrServerlessStartJobOperator(
        task_id="run_dq_circuit_breaker",
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver=get_spark_submit_config(
            "s3://ehdip-scripts/src/quality/dq_circuit_breaker.py",
            ["--input-table", "glue.silver_db.condition_occurrence", "--target-table", "glue.silver_db.condition_occurrence_valid", "--dlq-table", "glue.silver_db.condition_occurrence_dlq"]
        ),
        name="run_dq_circuit_breaker"
    )

    # 5. DBT Gold Marts (Simulated via Bash Operator for dbt-core)
    # In practice this runs a dbt project against Snowflake
    run_dbt_gold_marts = BashOperator(
        task_id='run_dbt_gold_marts',
        bash_command='dbt run --select models/gold --profiles-dir /path/to/profiles'
    )

    # Define Sequence
    ingest_x12_batch >> run_deid_engine >> run_omop_transformer >> run_dq_circuit_breaker >> run_dbt_gold_marts
