# src/ingestion/x12_batch_parser.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit, input_file_name

def get_spark_session():
    return SparkSession.builder \
        .appName("X12_EDI_Batch_Ingestion") \
        .getOrCreate()

def process_x12_batch(spark, s3_input_path, iceberg_table):
    """
    Parses X12 EDI files from S3 and ingests them into the Bronze Iceberg table.
    """
    # Read raw EDI lines as text
    df_raw = spark.read.text(s3_input_path)

    # In a real scenario, this would apply an EDI parsing library or custom UDF.
    # Here we treat the raw EDI segment string as the payload and wrap it in JSON.
    df_transformed = df_raw \
        .withColumn("payload_id", expr("uuid()")) \
        .withColumn("source_system_id", lit("x12_edi_batch")) \
        .withColumn("raw_payload_json", col("value")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .select("payload_id", "source_system_id", "raw_payload_json", "ingestion_timestamp")

    # Append to Bronze Iceberg
    df_transformed.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(iceberg_table)

if __name__ == "__main__":
    spark = get_spark_session()

    # Configuration
    S3_INPUT_PATH = "s3://ehdip-datalake-landing-123456789012/x12/837/*.edi"
    ICEBERG_TABLE = "glue_catalog.bronze.raw_x12_payload"

    process_x12_batch(spark, S3_INPUT_PATH, ICEBERG_TABLE)
