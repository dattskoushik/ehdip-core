import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, split, lit

def parse_x12(spark, input_path, output_path):
    # A simplified mock X12 parser for 837/835 lines (using ~ as segment separator)
    df = spark.read.text(input_path)

    parsed = df.select(split(col("value"), "~").alias("segments")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system_id", lit("X12_BATCH")) \
        .withColumn("payload_id", col("segments").getItem(0)) \
        .withColumn("raw_payload_json", col("value"))

    parsed.write.format("parquet").mode("append").save(output_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', required=True)
    parser.add_argument('--output_path', required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("X12 Batch Parser").getOrCreate()
    parse_x12(spark, args.input_path, args.output_path)
