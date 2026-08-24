from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite
import pydeequ

def run_data_quality_checks(spark, df):
    """Runs data quality checks and routes invalid records to DLQ."""

    check = Check(spark, CheckLevel.Error, "OMOP Data Quality Check")

    checkResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(
            check.isComplete("person_id")  # person_id cannot be null
            .isComplete("condition_concept_id") # Must have concept
            .isNonNegative("condition_concept_id")
            # Logical dates (no future dates for start)
            # Add more clinical plausibility checks
        ) \
        .run()

    checkResult_df = VerificationSuite.checkResultsAsDataFrame(spark, checkResult)

    # Analyze results
    status = checkResult_df.filter(checkResult_df.check_status == 'Error').count() == 0

    if not status:
        # Route to DLQ (Simplified logic: write entire batch to DLQ if error exists,
        # normally you would filter row-by-row or use a rules engine for row-level DLQ routing)
        print("Data Quality Checks FAILED. Routing to DLQ.")
        df.write.format("iceberg").mode("append").saveAsTable("glue_catalog.ehdip_dlq.omop_conditions")
        return False
    else:
        print("Data Quality Checks PASSED.")
        return True

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("EHDIP_DQ_Circuit_Breaker") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()

    df = spark.read.table("glue_catalog.ehdip_silver.condition_occurrence")
    run_data_quality_checks(spark, df)
