import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pydeequ.checks import *
from pydeequ.verification import *

def main(args):
    # PyDeequ needs specific packages configured
    spark = SparkSession.builder \
        .appName("EHDIP_DQ_Circuit_Breaker") \
        .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.3-spark-3.3") \
        .getOrCreate()

    # Read from Silver OMOP or De-id Bronze
    df = spark.read \
        .format("iceberg") \
        .load(f"{args.catalog}.{args.database}.{args.input_table}")

    # For the sake of this module, we validate the data before upserting it.
    # Structural rules and clinical plausibility
    # e.g. person_id must not be null, condition_start_date must be in the past

    check = Check(spark, CheckLevel.Error, "OMOP Data Quality Check")

    # We use pydeequ to verify the dataframe
    checkResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(
            check.hasSize(lambda x: x >= 1) \
            .isComplete("person_id") \
            .isComplete("condition_occurrence_id") \
            .isNonNegative("condition_concept_id") \
            # .satisfies("condition_start_date <= current_date()", "Condition start date in past")
        ) \
        .run()

    checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)

    # Simple logic to determine if it passes or fails
    # If any error level check failed, route entire batch to DLQ or route specific invalid records.
    # We will simulate a simple routing where we separate valid vs invalid rows based on basic spark filters
    # to emulate row-level DLQ routing since pydeequ is primarily for dataset-level aggregate metrics.

    valid_df = df.filter(col("person_id").isNotNull() & col("condition_occurrence_id").isNotNull())
    invalid_df = df.filter(col("person_id").isNull() | col("condition_occurrence_id").isNull())

    # Write Valid Data to Silver
    valid_df.write \
        .format("iceberg") \
        .mode("append") \
        .save(f"{args.catalog}.{args.database}.{args.valid_table}")

    # Write Invalid Data to DLQ (Dead Letter Queue)
    if invalid_df.count() > 0:
        invalid_df.write \
            .format("iceberg") \
            .mode("append") \
            .save(f"{args.catalog}.{args.database}.{args.dlq_table}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Data Quality Validation & DLQ Routing")
    parser.add_argument("--catalog", default="glue_catalog", help="Iceberg Catalog")
    parser.add_argument("--database", default="ehdip_silver_db", help="Database")
    parser.add_argument("--input-table", required=True, help="Input Table")
    parser.add_argument("--valid-table", required=True, help="Valid Output Table")
    parser.add_argument("--dlq-table", required=True, help="DLQ Table")

    args = parser.parse_args()
    main(args)
