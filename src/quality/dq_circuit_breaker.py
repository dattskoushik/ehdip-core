import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def configure_spark_for_deequ(spark):
    """
    Ensure SPARK_VERSION and proper JARs are set up for PyDeequ,
    although typically this is done via spark-submit arguments or EMR config.
    """
    os.environ["SPARK_VERSION"] = spark.version
    print(f"Configured PyDeequ for Spark Version: {spark.version}")

def run_dq_checks(spark, input_table, valid_output_table, dlq_output_table):
    import pydeequ
    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationSuite

    configure_spark_for_deequ(spark)

    df = spark.table(input_table)

    # Define data quality checks
    # Example OMOP checks: person_id is complete, condition_concept_id is not null
    check = Check(spark, CheckLevel.Error, "OMOP_Silver_DQ_Check")
    checkResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(
            check.isComplete("person_id")
            .isComplete("condition_concept_id")
            .isNonNegative("condition_concept_id")
        ) \
        .run()

    checkResult_df = VerificationSuite.checkResultsAsDataFrame(spark, checkResult)
    checkResult_df.show(truncate=False)

    # In a full circuit breaker, we filter row-by-row based on constraints
    # For PyDeequ, row-level validation (like filtering out bad rows) requires
    # more advanced setup (e.g. using RowLevelAnalyzer or filtering manually based on logic)

    # Simple simulated row-level filter for DLQ routing:
    valid_df = df.filter(col("person_id").isNotNull() & col("condition_concept_id").isNotNull())
    dlq_df = df.filter(col("person_id").isNull() | col("condition_concept_id").isNull())

    # Route to tables
    valid_df.createOrReplaceTempView("valid_updates")
    dlq_df.createOrReplaceTempView("dlq_updates")

    spark.sql(f"""
    MERGE INTO {valid_output_table} t
    USING valid_updates s
    ON t.condition_occurrence_id = s.condition_occurrence_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

    spark.sql(f"""
    MERGE INTO {dlq_output_table} t
    USING dlq_updates s
    ON t.condition_occurrence_id = s.condition_occurrence_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

    print("Data quality checks complete. Invalid records routed to DLQ.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="DQ Circuit Breaker & DLQ Routing")
    parser.add_argument("--input-table", required=True, help="Input Silver OMOP table")
    parser.add_argument("--valid-output-table", required=True, help="Target Silver verified table")
    parser.add_argument("--dlq-output-table", required=True, help="Target DLQ table")

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("EHDIP_DQ_Circuit_Breaker") \
        .getOrCreate()

    run_dq_checks(spark, args.input_table, args.valid_output_table, args.dlq_output_table)
