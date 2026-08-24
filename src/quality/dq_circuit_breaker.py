from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, expr
import sys
import os

# Set required environmental variables for PyDeequ
os.environ["SPARK_VERSION"] = "3.3"

def get_spark_session(app_name="DQ_Circuit_Breaker"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.3-spark-3.3") \
        .getOrCreate()

def run_dq_validation(spark, input_table, valid_table, dlq_table):
    """
    Validates data quality rules using PyDeequ.
    Records that fail validation are routed to DLQ.
    """
    # Import PyDeequ here to ensure it uses the properly configured SparkSession
    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationSuite
    import pydeequ

    df = spark.read.table(input_table)

    # Run PyDeequ verification
    # 1. Heart rate must be between 30 and 250 (assuming measurement_concept_id = 3027018)
    # 2. Event date cannot be in the future (measurement_date <= current_date)

    check = Check(spark, CheckLevel.Error, "Clinical Plausibility Check")

    checkResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(
            check.satisfies("measurement_concept_id != 3027018 OR (value_as_number >= 30 AND value_as_number <= 250)", "HR Bounds")
                 .satisfies("measurement_date <= current_date()", "No Future Dates")
        ) \
        .run()

    checkResult_df = VerificationSuite.checkResultsAsDataFrame(spark, checkResult)

    # If the status is not success, we can route the entire batch or use row-level evaluation
    # to find specific bad records. Since PyDeequ is column/dataset level aggregation,
    # we typically flag the batch or use it in conjunction with native filtering for row-level DLQ.
    # To meet the row-level DLQ requirement explicitly:
    cond_hr = "measurement_concept_id != 3027018 OR (value_as_number >= 30 AND value_as_number <= 250)"
    cond_date = "measurement_date <= current_date()"

    df_validated = df.withColumn("dq_passed", expr(f"({cond_hr}) AND ({cond_date})"))

    # Route valid data
    df_valid = df_validated.filter("dq_passed = True").drop("dq_passed")
    df_valid.write.format("iceberg").mode("append").saveAsTable(valid_table)

    # Route invalid data to DLQ
    df_dlq = df_validated.filter("dq_passed = False").drop("dq_passed")
    if df_dlq.count() > 0:
        df_dlq = df_dlq.withColumn("error_reason", lit("Failed PyDeequ clinical plausibility bounds or date checks"))
        df_dlq.write.format("iceberg").mode("append").saveAsTable(dlq_table)
        print(f"Routed {df_dlq.count()} bad records to DLQ: {dlq_table}")

    print(f"Validation complete. Good records written to {valid_table}")

def main():
    if len(sys.argv) < 4:
        print("Usage: dq_circuit_breaker.py <input_table> <valid_table> <dlq_table>")
        sys.exit(1)

    input_table = sys.argv[1]
    valid_table = sys.argv[2]
    dlq_table = sys.argv[3]

    spark = get_spark_session()
    run_dq_validation(spark, input_table, valid_table, dlq_table)

if __name__ == "__main__":
    main()
