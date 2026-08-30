import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, split, monotonically_increasing_id

def parse_x12(spark, input_path, output_table):
    # Read text file
    df = spark.read.text(input_path)

    # Very basic X12 split logic simulation for 837/835
    parsed_df = df.withColumn("segments", split(col("value"), "~")) \
        .withColumn("payload_id", monotonically_increasing_id().cast("string")) \
        .withColumn("raw_payload_json", col("value")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system_id", lit("x12_batch")) \
        .select("ingestion_timestamp", "source_system_id", "payload_id", "raw_payload_json")

    # Write to Bronze
    parsed_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(output_table)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True, help="Path to raw X12 files (e.g., s3://...)")
    parser.add_argument("--output_table", required=True, help="Iceberg table name")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("X12_Batch_Parser") \
        .getOrCreate()

    parse_x12(spark, args.input_path, args.output_table)
