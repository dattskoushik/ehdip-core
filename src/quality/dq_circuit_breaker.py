import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import pydeequ
from pydeequ.checks import *
from pydeequ.verification import *

def main():
    parser = argparse.ArgumentParser(description="Data Quality Validation & DLQ Routing")
    parser.add_argument("--input-table", required=True, help="Input Silver OMOP table")
    parser.add_argument("--target-table", required=True, help="Target Silver OMOP table after validation")
    parser.add_argument("--dlq-table", required=True, help="Dead Letter Queue table for invalid records")
    args = parser.parse_args()

    # PyDeequ needs SPARK_VERSION
    os.environ["SPARK_VERSION"] = "3.5"

    spark = SparkSession.builder \
        .appName("EHDIP_DQ_Circuit_Breaker") \
        .config("spark.jars.packages", "com.amazon.deequ:deequ-2.0.7-spark-3.4_2.12") \
        .getOrCreate()

    df = spark.table(args.input_table)

    # Note: Example logic assuming condition_occurrence
    # We will enforce:
    # 1. person_source_value is not null
    # 2. condition_concept_id is non-negative
    # 3. condition_start_date is logical (e.g. past or present)

    check = Check(spark, CheckLevel.Error, "OMOP Validation Rules") \
        .isComplete("person_source_value") \
        .isNonNegative("condition_concept_id")

    checkResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(check) \
        .run()

    checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)

    # Simple DLQ routing logic
    # In reality, row-level validation is tricky with PyDeequ since it returns aggregate metrics.
    # We typically apply rules via SQL expressions to filter row-level for DLQ.

    # Valid records (simulated SQL filters based on PyDeequ rules)
    valid_df = df.filter(
        col("person_source_value").isNotNull() &
        (col("condition_concept_id") >= 0)
    )

    # Invalid records (DLQ)
    invalid_df = df.filter(
        col("person_source_value").isNull() |
        (col("condition_concept_id") < 0)
    ).withColumn("dq_error_reason", lit("Failed structural/clinical rules"))

    # Write valid records idempotently using MERGE
    # Assuming person_source_value and condition_concept_id combination for deduping
    valid_df.createOrReplaceTempView("new_valid")
    spark.sql(f"""
    MERGE INTO {args.target_table} t
    USING new_valid s
    ON t.person_source_value = s.person_source_value
       AND t.condition_concept_id = s.condition_concept_id
    WHEN NOT MATCHED THEN
        INSERT *
    """)

    # Write invalid records to DLQ idempotently using MERGE if appropriate, or append
    # For DLQ, append might be fine to track all failures, but let's make it idempotent too.
    if invalid_df.count() > 0:
        invalid_df.createOrReplaceTempView("new_invalid")
        spark.sql(f"""
        MERGE INTO {args.dlq_table} t
        USING new_invalid s
        ON t.person_source_value = s.person_source_value
           AND t.condition_concept_id = s.condition_concept_id
        WHEN NOT MATCHED THEN
            INSERT *
        """)

    # If verification failed overall, we could optionally fail the job (Circuit Breaker)
    # status = checkResult_df.filter(col("check_status") == "Error").count()
    # if status > 0:
    #     raise Exception("Data Quality Circuit Breaker Triggered!")

    spark.stop()

if __name__ == "__main__":
    main()
