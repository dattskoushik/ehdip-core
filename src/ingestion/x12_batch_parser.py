import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, udf, col
from pyspark.sql.types import StringType
import uuid

def get_spark_session():
    """Initialize SparkSession configured for Iceberg batch processing."""
    return SparkSession.builder \
        .appName("EHDIP_X12_Batch_Parser") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue.warehouse", "s3://ehdip-data-lake-bronze/") \
        .getOrCreate()

# Stub for a complex X12 parsing logic
def parse_x12_segment(segment_str):
    # In a real scenario, this would handle 837/835 structural parsing
    return f"{{\"parsed_x12\": \"{segment_str.replace('~', '')}\"}}"

parse_x12_udf = udf(parse_x12_segment, StringType())

def process_x12_batch(input_path: str):
    """Read raw X12 EDI files, parse into JSON, and append to Bronze."""
    spark = get_spark_session()

    # Read raw EDI text files (each segment separated by ~)
    # Using text format for simple line-by-line reading for demonstration
    df_raw = spark.read.text(input_path)

    # Transform to Bronze schema
    df_bronze = df_raw.select(col("value").alias("raw_line")) \
        .withColumn("payload_id", udf(lambda: str(uuid.uuid4()), StringType())()) \
        .withColumn("source_system_id", lit("X12_EDI_BATCH")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("raw_payload_json", parse_x12_udf("raw_line")) \
        .select("payload_id", "source_system_id", "ingestion_timestamp", "raw_payload_json")

    # Append to Iceberg Bronze table
    df_bronze.write \
        .format("iceberg") \
        .mode("append") \
        .save("glue.ehdip_bronze.raw_payloads")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: x12_batch_parser.py <s3_input_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    process_x12_batch(input_path)
