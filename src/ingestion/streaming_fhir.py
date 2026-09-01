import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

def main(args):
    spark = SparkSession.builder \
        .appName("EHDIP_Streaming_FHIR_Ingestion") \
        .getOrCreate()

    # Define simple schema for FHIR R4 JSON
    # In production, a schema registry or fully evolved schema might be used
    fhir_schema = StructType([
        StructField("resourceType", StringType(), True),
        StructField("id", StringType(), True),
        # Assuming rest of payload can be cast or kept as string and evolved
        StructField("payload", StringType(), True)
    ])

    # Read stream from MSK
    # MSK configuration includes SASL/SCRAM authentication
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_bootstrap_servers) \
        .option("subscribe", args.kafka_topic) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{os.environ.get('MSK_USER')}\" password=\"{os.environ.get('MSK_PASS')}\";") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON from Kafka value
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .withColumn("data", from_json(col("json_str"), fhir_schema)) \
        .select(
            col("data.id").alias("payload_id"),
            col("data.resourceType").alias("source_system_id"),
            col("json_str").alias("raw_payload_json"),
            current_timestamp().alias("ingestion_timestamp")
        )

    # Write stream to Iceberg Bronze table
    # Schema evolution could be configured at the iceberg level via mergeSchema option
    query = parsed_df \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("path", f"{args.catalog}.{args.database}.{args.table}") \
        .option("checkpointLocation", args.checkpoint_dir) \
        .start()

    query.awaitTermination()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Streaming FHIR Ingestion to Iceberg")
    parser.add_argument("--kafka-bootstrap-servers", required=True, help="MSK Bootstrap Servers")
    parser.add_argument("--kafka-topic", required=True, help="Kafka Topic")
    parser.add_argument("--catalog", default="glue_catalog", help="Iceberg Catalog")
    parser.add_argument("--database", default="ehdip_bronze_db", help="Iceberg Database")
    parser.add_argument("--table", default="raw_payloads", help="Iceberg Table")
    parser.add_argument("--checkpoint-dir", required=True, help="Checkpoint Directory for Structured Streaming")

    args = parser.parse_args()
    main(args)
