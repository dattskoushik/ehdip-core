import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import StringType, StructType, StructField

def main():
    parser = argparse.ArgumentParser(description="Debezium CDC Consumer to Bronze Iceberg")
    parser.add_argument("--kafka-brokers", required=True, help="Kafka broker connection string")
    parser.add_argument("--kafka-topic", required=True, help="Kafka topic to consume CDC logs from")
    parser.add_argument("--bronze-table", required=True, help="Target Bronze Iceberg table")
    parser.add_argument("--checkpoint-location", required=True, help="S3 path for streaming checkpoint")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("EHDIP_CDC_Consumer") \
        .getOrCreate()

    # Debezium CDC payload structure
    debezium_schema = StructType([
        StructField("payload", StructType([
            StructField("before", StringType(), True),
            StructField("after", StringType(), True),
            StructField("op", StringType(), True),
            StructField("source", StructType([
                StructField("ts_ms", StringType(), True)
            ]), True)
        ]), True)
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_brokers) \
        .option("subscribe", args.kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse CDC and filter relevant operations
    parsed_df = df.selectExpr("CAST(value AS STRING) as value_str") \
        .withColumn("debezium", from_json(col("value_str"), debezium_schema)) \
        .filter(col("debezium.payload.op").isin("c", "u", "d")) \
        .select(
            current_timestamp().alias("ingestion_timestamp"),
            lit("DEBEZIUM_CDC").alias("source_system_id"),
            # For simplistic raw storage, we just dump the JSON payload
            col("value_str").alias("raw_payload_json")
        ) \
        .withColumn("payload_id", expr("uuid()")) \
        .select("ingestion_timestamp", "source_system_id", "payload_id", "raw_payload_json")

    query = parsed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", args.checkpoint_location) \
        .toTable(args.bronze_table)

    query.awaitTermination()

if __name__ == "__main__":
    main()
