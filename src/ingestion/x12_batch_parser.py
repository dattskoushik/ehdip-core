from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, expr
import sys

def get_spark_session(app_name="X12_EDI_Batch_Parser"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.type", "glue") \
        .getOrCreate()

def process_x12_batch(spark, input_path, output_table):
    # Read raw text lines
    df_raw = spark.read.text(input_path)

    # Simplified parsing for X12 (splitting by ~)
    # In a real scenario, a dedicated X12 parser library would be used
    df_parsed = df_raw \
        .withColumn("payload_id", expr("uuid()")) \
        .withColumn("source_system_id", lit("EDI_X12_BATCH")) \
        .withColumn("raw_payload_json", col("value")) \
        .withColumn("ingestion_timestamp", current_timestamp())

    df_final = df_parsed.select("payload_id", "source_system_id", "raw_payload_json", "ingestion_timestamp")

    # Append to Bronze Iceberg table
    df_final.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(output_table)

    print(f"Successfully processed X12 batch from {input_path} into {output_table}")

def main():
    if len(sys.argv) < 3:
        print("Usage: x12_batch_parser.py <input_s3_path> <output_iceberg_table>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_table = sys.argv[2]

    spark = get_spark_session()
    process_x12_batch(spark, input_path, output_table)

if __name__ == "__main__":
    main()
