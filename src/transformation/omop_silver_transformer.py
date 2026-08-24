from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

def transform_to_omop_condition(deid_df: DataFrame) -> DataFrame:
    """Maps de-identified raw events to OMOP condition_occurrence table."""
    # Simplified mapping assuming the source DataFrame has standard abstracted columns
    # In reality, this would involve Athena vocabulary joins (concept_id lookups)
    return deid_df.select(
        col("payload_id").alias("condition_occurrence_id"),
        col("patient_id").alias("person_id"),
        col("condition_concept_id"),
        col("encounter_date_shifted").alias("condition_start_date"),
        col("encounter_date_shifted").alias("condition_start_datetime"),
        lit(None).cast("date").alias("condition_end_date"),
        lit(None).cast("timestamp").alias("condition_end_datetime"),
        lit(32020).alias("condition_type_concept_id"), # e.g., EHR encounter diagnosis
        lit(None).cast("string").alias("stop_reason"),
        lit(None).cast("integer").alias("provider_id"),
        col("encounter_id").alias("visit_occurrence_id"),
        lit(None).cast("integer").alias("visit_detail_id"),
        col("condition_source_value"),
        col("condition_source_concept_id"),
        lit(None).cast("string").alias("condition_status_source_value"),
        lit(None).cast("integer").alias("condition_status_concept_id")
    ).withColumn("transformed_at", current_timestamp())

def run_silver_transformation():
    spark = SparkSession.builder \
        .appName("EHDIP_OMOP_Silver_Transformer") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()

    # Read from Bronze (post de-id)
    deid_df = spark.read.table("glue_catalog.ehdip_bronze.deidentified_payloads")

    transform_to_omop_condition(deid_df).write.format("iceberg").mode("append").saveAsTable("glue_catalog.ehdip_silver.condition_occurrence")
    print("Silver transformation completed.")

if __name__ == "__main__":
    run_silver_transformation()
