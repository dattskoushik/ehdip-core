import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

def process_cdc(spark, input_path, output_table):
    cdc_schema = StructType([
        StructField("payload", StructType([
            StructField("op", StringType(), True),
            StructField("after", StringType(), True),
            StructField("before", StringType(), True)
        ]), True)
    ])

    df = spark.read.json(input_path, schema=cdc_schema)

    # Filter for Debezium CDC operations
    filtered_df = df.filter(col("payload.op").isin("c", "u", "d"))

    bronze_df = filtered_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system_id", col("payload.op")) \
        .withColumn("payload_id", col("payload.after")) \
        .withColumn("raw_payload_json", col("payload.after")) \
        .select("ingestion_timestamp", "source_system_id", "payload_id", "raw_payload_json")

    bronze_df.write.format("iceberg").mode("append").saveAsTable(output_table)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', required=True)
    parser.add_argument('--output_table', required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("CDC Consumer").getOrCreate()
    process_cdc(spark, args.input_path, args.output_table)
