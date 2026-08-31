import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, MapType

def main(args):
    spark = SparkSession.builder \
        .appName("EHDIP_CDC_Consumer") \
        .getOrCreate()

    # Read Debezium CDC logs (e.g., from Kafka or S3 dump)
    # Schema matches general Debezium payload structure
    debezium_schema = StructType([
        StructField("payload", StructType([
            StructField("before", MapType(StringType(), StringType()), True),
            StructField("after", MapType(StringType(), StringType()), True),
            StructField("source", MapType(StringType(), StringType()), True),
            StructField("op", StringType(), True) # 'c', 'u', 'd', 'r'
        ]), True)
    ])

    raw_cdc_df = spark.read.text(args.input_path)

    parsed_cdc_df = raw_cdc_df \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .withColumn("data", from_json(col("json_str"), debezium_schema)) \
        .filter(col("data.payload.op").isin('c', 'u', 'd'))

    bronze_df = parsed_cdc_df \
        .select(
            col("data.payload.after.id").alias("payload_id"),
            col("data.payload.source.db").alias("source_system_id"),
            col("json_str").alias("raw_payload_json"),
            current_timestamp().alias("ingestion_timestamp")
        )

    # Write to Bronze Iceberg table
    bronze_df.write \
        .format("iceberg") \
        .mode("append") \
        .save(f"{args.catalog}.{args.database}.{args.table}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Debezium CDC Consumer to Iceberg")
    parser.add_argument("--input-path", required=True, help="Path to raw CDC JSON logs")
    parser.add_argument("--catalog", default="glue_catalog", help="Iceberg Catalog")
    parser.add_argument("--database", default="ehdip_bronze_db", help="Iceberg Database")
    parser.add_argument("--table", default="raw_payloads", help="Iceberg Table")

    args = parser.parse_args()
    main(args)
