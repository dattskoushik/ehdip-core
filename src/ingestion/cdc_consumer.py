from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType
import sys

def get_spark_session(app_name="Debezium_CDC_Consumer"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.type", "glue") \
        .getOrCreate()

def process_cdc_batch(spark, input_path, output_table):
    # Debezium CDC records generally have 'before', 'after', 'op'
    # We load raw JSON, extract CDC operation metadata, and land in Bronze.
    df_raw = spark.read.json(input_path)

    # Assuming typical Debezium payload structure
    df_parsed = df_raw \
        .withColumn("payload_id", expr("uuid()")) \
        .withColumn("source_system_id", lit("DEBEZIUM_CDC")) \
        .withColumn("raw_payload_json", expr("to_json(struct(*))")) \
        .withColumn("ingestion_timestamp", current_timestamp())

    df_final = df_parsed.select("payload_id", "source_system_id", "raw_payload_json", "ingestion_timestamp")

    # Append to Bronze
    df_final.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(output_table)

    print(f"Successfully processed CDC batch from {input_path} into {output_table}")

def main():
    if len(sys.argv) < 3:
        print("Usage: cdc_consumer.py <input_s3_path> <output_iceberg_table>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_table = sys.argv[2]

    spark = get_spark_session()
    process_cdc_batch(spark, input_path, output_table)

if __name__ == "__main__":
    main()
