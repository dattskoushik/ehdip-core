# src/quality/dq_circuit_breaker.py

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

# Ensure PyDeequ dependencies are included in the spark session
# e.g. spark-submit --packages com.amazon.deequ:deequ:2.0.3-spark-3.3

def get_spark_session():
    return SparkSession.builder \
        .appName("DQ_Circuit_Breaker") \
        .getOrCreate()

def run_dq_and_route(spark, input_table, valid_output_table, dlq_output_table):
    """
    Validates data. Good records go to valid_output_table.
    Bad records are routed to a Dead Letter Queue (DLQ) table.
    """
    df_input = spark.table(input_table)

    # Optional: If PyDeequ is available, we would use VerificationSuite
    # from pydeequ.checks import *
    # from pydeequ.verification import *

    # We will tag rows with errors
    df_with_errors = df_input \
        .withColumn("dq_error_null_id", col("person_id").isNull()) \
        .withColumn("dq_error_future_date", col("birth_date") > current_timestamp().cast("date"))

    if "heart_rate" in df_with_errors.columns:
        df_with_errors = df_with_errors.withColumn(
            "dq_error_invalid_vitals",
            (col("heart_rate") < 0) | (col("heart_rate") > 300)
        )
    else:
        df_with_errors = df_with_errors.withColumn("dq_error_invalid_vitals", lit(False))

    # Combine errors to determine if valid
    df_evaluated = df_with_errors.withColumn(
        "is_valid",
        ~(col("dq_error_null_id") | col("dq_error_future_date") | col("dq_error_invalid_vitals"))
    )

    # Split Data
    df_valid = df_evaluated.filter(col("is_valid")).drop("dq_error_null_id", "dq_error_future_date", "dq_error_invalid_vitals", "is_valid")

    # For DLQ, create an error manifest column
    # In practice, this would serialize the specific failed rules to a JSON string
    df_dlq = df_evaluated.filter(~col("is_valid")) \
        .withColumn("dlq_timestamp", current_timestamp()) \
        .withColumn("error_manifest", lit("Validation failed for one or more rules: null_id, future_date, or invalid_vitals"))

    # Write Valid Records
    df_valid.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(valid_output_table)

    # Write Bad Records to DLQ
    df_dlq.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(dlq_output_table)

if __name__ == "__main__":
    spark = get_spark_session()

    INPUT_TABLE = "glue_catalog.silver.omop_person_raw"
    VALID_TABLE = "glue_catalog.silver.omop_person_validated"
    DLQ_TABLE = "glue_catalog.silver.dq_quarantine_dlq"

    run_dq_and_route(spark, INPUT_TABLE, VALID_TABLE, DLQ_TABLE)
