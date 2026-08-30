import os
import argparse
from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

def validate_and_route(spark, input_table, valid_output, dlq_output):
    df = spark.read.table(input_table)

    # Define checks
    check = Check(spark, CheckLevel.Error, "DQ Checks")
    checkResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(
            check.hasSize(lambda x: x > 0) \
            .isComplete("person_id") \
            .isComplete("condition_concept_id") \
            .isNonNegative("condition_concept_id")
        ) \
        .run()

    # Extract results
    checkResult_df = VerificationSuite.checkResultsAsDataFrame(spark, checkResult)
    status = checkResult_df.filter(checkResult_df.check_status == 'Error').count()

    # In a real engine, we'd use row-level evaluation or expectations to route specific failed rows
    # Here, for demo purposes, if the suite fails, we route all to DLQ, else to valid
    if status > 0:
        print("Validation FAILED. Routing to DLQ.")
        df.write.format("iceberg").mode("append").saveAsTable(dlq_output)
    else:
        print("Validation PASSED. Routing to valid table.")
        df.write.format("iceberg").mode("append").saveAsTable(valid_output)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", required=True)
    parser.add_argument("--valid_output", required=True)
    parser.add_argument("--dlq_output", required=True)
    args = parser.parse_args()

    # Set SPARK_VERSION for pydeequ compatibility if needed
    os.environ["SPARK_VERSION"] = "3.4"

    spark = SparkSession.builder \
        .appName("DQ_Circuit_Breaker") \
        .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.4") \
        .getOrCreate()

    validate_and_route(spark, args.input_table, args.valid_output, args.dlq_output)
