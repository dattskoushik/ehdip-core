import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, expr
from pyspark.sql.types import StringType, StructType, StructField

def main():
    parser = argparse.ArgumentParser(description="Streaming Ingestion for FHIR R4 via MSK to Bronze Iceberg")
    parser.add_argument("--kafka-brokers", required=True, help="Kafka broker connection string")
    parser.add_argument("--kafka-topic", required=True, help="Kafka topic to consume from")
    parser.add_argument("--bronze-table", required=True, help="Target Bronze Iceberg table (e.g., catalog.bronze_db.raw_payloads)")
    parser.add_argument("--checkpoint-location", required=True, help="S3 path for streaming checkpoint")
    args = parser.parse_args()

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("EHDIP_Streaming_FHIR_Ingestion") \
        .getOrCreate()

    # Define simple schema for incoming Kafka message
    # Expecting message value to be a JSON string representing FHIR R4
    kafka_schema = StructType([
        StructField("payload_id", StringType(), True),
        StructField("fhir_json", StringType(), True)
    ])

    # Read stream from Kafka (AWS MSK with SASL/SCRAM assumed configured in cluster environment via spark-submit)
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_brokers) \
        .option("subscribe", args.kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Process streaming dataframe
    # Kafka message value is binary, cast to string
    processed_df = df.selectExpr("CAST(value AS STRING) as value_str") \
        .withColumn("data", from_json(col("value_str"), kafka_schema)) \
        .select(
            current_timestamp().alias("ingestion_timestamp"),
            expr("'MSK_FHIR_STREAM'").alias("source_system_id"),
            col("data.payload_id").alias("payload_id"),
            col("data.fhir_json").alias("raw_payload_json")
        )

    # Write stream to Bronze Iceberg table
    query = processed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", args.checkpoint_location) \
        .toTable(args.bronze_table)

    query.awaitTermination()

if __name__ == "__main__":
    main()
