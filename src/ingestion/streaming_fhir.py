# src/ingestion/streaming_fhir.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr

def get_spark_session():
    return SparkSession.builder \
        .appName("FHIR_Streaming_Ingestion") \
        .getOrCreate()

def process_stream(spark, kafka_bootstrap_servers, kafka_topic, checkpoint_location, iceberg_table):
    """
    Consumes FHIR R4 JSON from AWS MSK (Kafka) and appends to Bronze Iceberg table.
    """

    # Read from Kafka (MSK with SASL/SCRAM authentication simulation)
    # Note: For real MSK, we would configure sasl.jaas.config, sasl.mechanism, security.protocol
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse Kafka payload and construct Bronze record
    # The 'value' column from Kafka is a binary, we cast it to string which is our raw_payload_json
    transformed_df = df_stream.selectExpr("CAST(value AS STRING) as raw_payload_json") \
        .withColumn("payload_id", expr("uuid()")) \
        .withColumn("source_system_id", expr("'msk_fhir_stream'")) \
        .withColumn("ingestion_timestamp", current_timestamp())

    # Reorder columns to match Iceberg table schema:
    # payload_id, source_system_id, raw_payload_json, ingestion_timestamp
    final_df = transformed_df.select(
        "payload_id",
        "source_system_id",
        "raw_payload_json",
        "ingestion_timestamp"
    )

    # Write stream to Iceberg Bronze table
    query = final_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", checkpoint_location) \
        .toTable(iceberg_table)

    return query

if __name__ == "__main__":
    spark = get_spark_session()

    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = "b-1.ehdipmsk.xyz.c2.kafka.us-east-1.amazonaws.com:9096,b-2.ehdipmsk.xyz.c2.kafka.us-east-1.amazonaws.com:9096"
    KAFKA_TOPIC = "fhir_raw_stream"
    CHECKPOINT_LOCATION = "s3://ehdip-datalake-bronze-123456789012/checkpoints/fhir_stream"
    ICEBERG_TABLE = "glue_catalog.bronze.raw_fhir_payload"

    query = process_stream(spark, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, CHECKPOINT_LOCATION, ICEBERG_TABLE)
    query.awaitTermination()
