from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, expr
from pyspark.sql.types import StringType

def process_fhir_stream(spark: SparkSession, kafka_bootstrap_servers: str, kafka_topic: str, checkpoint_location: str, iceberg_table: str):
    """
    Consumes FHIR R4 JSON from MSK (Kafka), wraps it in audit metadata, and appends to Bronze Iceberg table.
    """
    # Read stream from MSK/Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .load()

    # Cast value to string (JSON payload)
    parsed_df = df.selectExpr("CAST(value AS STRING) as raw_payload_json")

    # Add audit metadata
    enriched_df = parsed_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system_id", lit("kafka_fhir_stream")) \
        .withColumn("payload_id", expr("uuid()"))

    # Write to Bronze Iceberg table
    query = enriched_df \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .toTable(iceberg_table)

    return query

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Streaming_FHIR_Ingestion") \
        .getOrCreate()

    KAFKA_BOOTSTRAP = "localhost:9092"
    TOPIC = "fhir_raw"
    CHECKPOINT = "s3://ehdip-bronze-data-lake/checkpoints/fhir_stream"
    TABLE = "ehdip_catalog.ehdip.bronze_raw_payload"

    # query = process_fhir_stream(spark, KAFKA_BOOTSTRAP, TOPIC, CHECKPOINT, TABLE)
    # query.awaitTermination()
