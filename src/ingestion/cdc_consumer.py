from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, uuid, lit
from pyspark.sql.types import StructType, StructField, StringType, MapType

def run_cdc_ingestion(input_path: str, table_name: str):
    spark = SparkSession.builder \
        .appName("EHDIP_Debezium_CDC_Consumer") \
        .getOrCreate()

    # Define Debezium CDC Schema wrapper
    debezium_schema = StructType([
        StructField("op", StringType(), True),
        StructField("before", StringType(), True),
        StructField("after", StringType(), True),
        StructField("source", MapType(StringType(), StringType()), True)
    ])

    raw_cdc_df = spark.read.json(input_path, schema=debezium_schema)

    # Filter for C, U, D operations
    filtered_cdc = raw_cdc_df.filter(col("op").isin(["c", "u", "d"]))

    # Convert to Bronze format
    bronze_df = filtered_cdc \
        .withColumn("raw_payload_json", col("after")) \
        .withColumn("payload_id", uuid()) \
        .withColumn("source_system_id", lit("debezium-cdc")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .select("payload_id", "source_system_id", "raw_payload_json", "ingestion_timestamp")

    bronze_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(table_name)

if __name__ == "__main__":
    run_cdc_ingestion("s3://ehdip-landing/cdc/", "ehdip.ehdip_bronze.raw_payloads")
