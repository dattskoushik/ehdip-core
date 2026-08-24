from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, expr, explode, split
import uuid

def process_x12_batch(s3_input_path: str):
    spark = SparkSession.builder \
        .appName("EHDIP_X12_Batch_Parser") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()

    # Read raw X12 as text lines
    raw_df = spark.read.text(s3_input_path)

    # Simplified parsing for demonstration (assume '~' separated segments)
    segments_df = raw_df.select(explode(split(col("value"), "~")).alias("segment")) \
        .filter(col("segment") != "")

    # Construct the JSON payload mapping for Bronze
    processed_df = segments_df.selectExpr("segment as raw_payload_json") \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system_id", lit("x12_batch_ftp")) \
        .withColumn("payload_id", expr("uuid()"))

    # Write to Bronze Iceberg
    processed_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("glue_catalog.ehdip_bronze.raw_payloads")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        process_x12_batch(sys.argv[1])
    else:
        print("Provide S3 input path")
