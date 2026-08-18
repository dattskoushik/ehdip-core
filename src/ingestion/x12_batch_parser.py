from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, uuid, lit

def run_x12_batch_ingestion(input_path: str, table_name: str):
    spark = SparkSession.builder \
        .appName("EHDIP_X12_Batch_Ingestion") \
        .getOrCreate()

    # Read raw X12 as text lines
    raw_df = spark.read.text(input_path)

    # Process into Bronze format
    bronze_df = raw_df \
        .withColumnRenamed("value", "raw_payload_json") \
        .withColumn("payload_id", uuid()) \
        .withColumn("source_system_id", lit("x12-batch-parser")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .select("payload_id", "source_system_id", "raw_payload_json", "ingestion_timestamp")

    # Append to Bronze Iceberg
    bronze_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(table_name)

if __name__ == "__main__":
    run_x12_batch_ingestion("s3://ehdip-landing/x12/", "ehdip.ehdip_bronze.raw_payloads")
