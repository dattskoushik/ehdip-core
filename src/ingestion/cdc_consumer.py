import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, MapType

def get_spark_session():
    """Initialize SparkSession configured for Iceberg batch processing."""
    return SparkSession.builder \
        .appName("EHDIP_CDC_Consumer") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue.warehouse", "s3://ehdip-data-lake-bronze/") \
        .getOrCreate()

def process_cdc_batch(input_path: str):
    """Read Debezium CDC change logs and append to Bronze."""
    spark = get_spark_session()

    # Define Debezium envelope schema
    debezium_schema = StructType([
        StructField("op", StringType(), True), # c, u, d
        StructField("before", StringType(), True), # Struct represented as JSON string for simplicity here
        StructField("after", StringType(), True),
        StructField("source", MapType(StringType(), StringType()), True)
    ])

    # Read CDC JSON files
    df_cdc = spark.read.schema(debezium_schema).json(input_path)

    # Filter for relevant operations and format as Bronze payload
    df_bronze = df_cdc.filter(col("op").isin("c", "u", "d")) \
        .withColumn("payload_id", col("source.lsn").cast(StringType())) \
        .withColumn("source_system_id", col("source.db")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("raw_payload_json", to_json(struct("op", "before", "after", "source"))) \
        .select("payload_id", "source_system_id", "ingestion_timestamp", "raw_payload_json")

    # Append to Iceberg Bronze table
    df_bronze.write \
        .format("iceberg") \
        .mode("append") \
        .save("glue.ehdip_bronze.raw_payloads")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: cdc_consumer.py <s3_input_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    process_cdc_batch(input_path)
