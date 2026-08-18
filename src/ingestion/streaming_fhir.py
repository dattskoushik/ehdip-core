from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, expr, uuid
from pyspark.sql.types import StringType

def run_streaming_ingestion():
    spark = SparkSession.builder \
        .appName("EHDIP_FHIR_Streaming_Ingestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.ehdip", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.ehdip.type", "glue") \
        .getOrCreate()

    # Kafka MSK Configuration (SASL/SCRAM for HIPAA)
    kafka_brokers = "b-1.msk-cluster.amazonaws.com:9096,b-2.msk-cluster.amazonaws.com:9096"
    kafka_topic = "fhir-events"

    # Read streaming data from MSK
    # In production, specify security.protocol and sasl.mechanism
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Process stream: Cast value to string, add metadata
    processed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as raw_payload_json") \
        .withColumn("payload_id", uuid()) \
        .withColumn("source_system_id", expr("'kafka-msk-fhir'")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .select("payload_id", "source_system_id", "raw_payload_json", "ingestion_timestamp")

    # Write stream to Iceberg Bronze table
    query = processed_stream.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", "s3://ehdip-bronze/checkpoints/fhir_streaming") \
        .toTable("ehdip.ehdip_bronze.raw_payloads")

    query.awaitTermination()

if __name__ == "__main__":
    run_streaming_ingestion()
