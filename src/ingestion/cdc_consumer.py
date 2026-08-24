from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, lit, expr, when
from pyspark.sql.types import StructType, StructField, StringType, LongType
import os

def process_cdc_batch(s3_input_path: str):
    spark = SparkSession.builder \
        .appName("EHDIP_CDC_Consumer_Batch") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()

    # Read CDC events from S3 (batch export from Debezium)
    df = spark.read.text(s3_input_path)

    debezium_schema = StructType([
        StructField("payload", StructType([
            StructField("before", StringType()),
            StructField("after", StringType()),
            StructField("op", StringType()), # c, u, d
            StructField("ts_ms", LongType())
        ]))
    ])

    parsed_df = df.selectExpr("CAST(value AS STRING) as json_val") \
        .select(from_json("json_val", debezium_schema).alias("data")) \
        .select("data.payload.*")

    # Transform to raw payload layout, storing the CDC event as the payload
    # For deletes, we might store 'before', for creates/updates we store 'after'
    processed_df = parsed_df.withColumn("raw_payload_json",
                                        when(col("op") == "d", col("before")).otherwise(col("after"))) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system_id", lit("debezium_cdc")) \
        .withColumn("payload_id", expr("uuid()")) \
        .select("ingestion_timestamp", "source_system_id", "payload_id", "raw_payload_json")

    processed_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("glue_catalog.ehdip_bronze.raw_payloads")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        process_cdc_batch(sys.argv[1])
    else:
        print("Provide S3 input path for CDC batch")
