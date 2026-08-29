import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, struct, to_json
from pyspark.sql.types import StringType

def main(kafka_brokers, kafka_topic, iceberg_table, checkpoint_location):
    # EMR Serverless applications need a spark session initialized
    spark = SparkSession.builder \
        .appName("EHDIP_Streaming_FHIR_Ingestion") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read from MSK / Kafka (SASL/SCRAM assumed configured in env)
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Add audit metadata
    bronze_df = df.select(
        current_timestamp().alias("ingestion_timestamp"),
        lit("MSK_FHIR_STREAM").alias("source_system_id"),
        col("key").cast(StringType()).alias("payload_id"),
        col("value").cast(StringType()).alias("raw_payload_json")
    )

    # Write to Bronze Iceberg table using append
    # In streaming context, append is standard for bronze ingestion
    query = bronze_df \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .toTable(iceberg_table)

    query.awaitTermination()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Streaming FHIR Ingestion from MSK to Iceberg")
    parser.add_argument("--kafka-brokers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", required=True, help="Kafka topic to consume")
    parser.add_argument("--iceberg-table", required=True, help="Target Iceberg table (e.g. ehdip_bronze.bronze_raw_payload)")
    parser.add_argument("--checkpoint-location", required=True, help="S3 path for stream checkpointing")

    args = parser.parse_args()

    main(args.kafka_brokers, args.kafka_topic, args.iceberg_table, args.checkpoint_location)
