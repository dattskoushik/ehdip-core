import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit, uuid

def get_spark_session():
    """Initialize SparkSession configured for Iceberg and Kafka MSK (SASL/SCRAM)."""
    return SparkSession.builder \
        .appName("EHDIP_Streaming_FHIR_Ingestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue.warehouse", "s3://ehdip-data-lake-bronze/") \
        .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()

def process_fhir_stream():
    """Consume FHIR R4 JSON from Kafka/MSK and append to Bronze Iceberg table."""
    spark = get_spark_session()

    # Kafka connection properties (Assuming SASL/SCRAM for MSK)
    kafka_brokers = os.environ.get("KAFKA_BROKERS", "localhost:9092")
    kafka_topic = "fhir_incoming"

    # Read stream from Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config", os.environ.get("KAFKA_JAAS_CONFIG", "")) \
        .load()

    # Transform raw Kafka record into Bronze schema
    df_bronze = df_kafka.selectExpr("CAST(value AS STRING) as raw_payload_json") \
        .withColumn("payload_id", expr("uuid()")) \
        .withColumn("source_system_id", lit("KAFKA_FHIR_STREAM")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .select("payload_id", "source_system_id", "ingestion_timestamp", "raw_payload_json")

    # Write to Iceberg Bronze table
    # Requires checkpoint location for reliability
    checkpoint_location = "s3://ehdip-data-lake-bronze/checkpoints/streaming_fhir/"

    query = df_bronze.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("path", "glue.ehdip_bronze.raw_payloads") \
        .option("checkpointLocation", checkpoint_location) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    process_fhir_stream()
