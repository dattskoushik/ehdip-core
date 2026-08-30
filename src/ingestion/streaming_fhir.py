import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def process_stream(spark, brokers, topic, output_table):
    # Consume from Kafka/MSK
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("subscribe", topic) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("startingOffsets", "latest") \
        .load()

    # Define metadata columns
    parsed_df = df.selectExpr("CAST(key AS STRING) as payload_id", "CAST(value AS STRING) as raw_payload_json") \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system_id", lit("kafka_msk_fhir_stream"))

    # Write stream to Bronze Iceberg
    query = parsed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", f"s3://ehdip-checkpoint/bronze/{output_table}") \
        .toTable(output_table)

    query.awaitTermination()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--topic", required=True, help="Kafka topic")
    parser.add_argument("--output_table", required=True, help="Iceberg table name (e.g., ehdip_bronze_db.raw_payloads)")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("Streaming_FHIR_Ingestion") \
        .getOrCreate()

    process_stream(spark, args.brokers, args.topic, args.output_table)
