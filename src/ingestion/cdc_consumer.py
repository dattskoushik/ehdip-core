import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, MapType

def consume_cdc(spark, input_path, output_table):
    # Read Debezium JSON CDC logs
    # Assume schema has 'op' for operation and 'after' for payload
    schema = StructType([
        StructField("op", StringType(), True),
        StructField("after", MapType(StringType(), StringType()), True),
        StructField("before", MapType(StringType(), StringType()), True),
        StructField("source", MapType(StringType(), StringType()), True)
    ])

    df = spark.read.json(input_path, schema=schema)

    # Filter for C, U, D ops
    cdc_df = df.filter(col("op").isin("c", "u", "d"))

    # Extract payload and metadata
    parsed_df = cdc_df.withColumn("payload_id", col("after.id")) \
        .withColumn("raw_payload_json", to_json(col("after"))) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system_id", lit("debezium_cdc")) \
        .select("ingestion_timestamp", "source_system_id", "payload_id", "raw_payload_json")

    # Write to Bronze Iceberg
    parsed_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(output_table)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True, help="Path to Debezium JSON logs")
    parser.add_argument("--output_table", required=True, help="Iceberg table name")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("CDC_Consumer") \
        .getOrCreate()

    consume_cdc(spark, args.input_path, args.output_table)
