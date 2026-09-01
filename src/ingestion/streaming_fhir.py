import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

def main(kafka_brokers, kafka_topic, iceberg_table, checkpoint_location):
    spark = SparkSession.builder \
        .appName("FHIR Streaming Ingestion") \
        .getOrCreate()

    # Kafka connection and properties (mocked for Serverless / SASL SCRAM)
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", kafka_topic) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .load()

    # Simplified FHIR schema for parsing payload
    fhir_schema = StructType([
        StructField("resourceType", StringType(), True),
        StructField("id", StringType(), True)
    ])

    parsed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system_id", col("key")) \
        .withColumn("payload_id", from_json(col("value"), fhir_schema).getField("id")) \
        .withColumnRenamed("value", "raw_payload_json") \
        .select("ingestion_timestamp", "source_system_id", "payload_id", "raw_payload_json")

    query = parsed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", checkpoint_location) \
        .toTable(iceberg_table)

    query.awaitTermination()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafka_brokers', required=True)
    parser.add_argument('--kafka_topic', required=True)
    parser.add_argument('--iceberg_table', required=True)
    parser.add_argument('--checkpoint_location', required=True)
    args = parser.parse_args()

    main(args.kafka_brokers, args.kafka_topic, args.iceberg_table, args.checkpoint_location)
