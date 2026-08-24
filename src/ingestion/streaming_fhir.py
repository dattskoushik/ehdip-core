import sys
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit, udf
from pyspark.sql.types import StringType

def generate_uuid():
    return str(uuid.uuid4())

uuid_udf = udf(generate_uuid, StringType())

def process_fhir_stream(spark: SparkSession, msk_brokers: str, topic: str, bronze_table: str):
    """
    Consumes FHIR R4 JSON from MSK (SASL/SCRAM) and appends to Bronze Iceberg.
    """
    # Read stream from Kafka (AWS MSK)
    # In a real environment, the SECURE_PASSWORD would be dynamically fetched from AWS Secrets Manager
    # before the Spark job launches, passing it in securely via environment variables or Spark configurations.
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", msk_brokers) \
        .option("subscribe", topic) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='ehdip_streaming_user' password='[SECURE_PASSWORD]';") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse and transform payload to Bronze schema
    # Kafka value contains the raw JSON payload
    df_bronze = df_raw.selectExpr("CAST(value AS STRING) as raw_payload_json") \
        .withColumn("payload_id", uuid_udf()) \
        .withColumn("source_system_id", lit("MSK_FHIR_STREAM")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .select("payload_id", "source_system_id", "ingestion_timestamp", "raw_payload_json")

    # Write stream to Bronze Iceberg table
    query = df_bronze.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", f"s3://ehdip-bronze-data-lake/checkpoints/fhir_stream/") \
        .toTable(bronze_table)

    return query

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: streaming_fhir.py <msk_brokers> <topic> <bronze_table>")
        sys.exit(1)

    msk_brokers = sys.argv[1]
    topic = sys.argv[2]
    bronze_table = sys.argv[3]

    spark = SparkSession.builder \
        .appName("EHDIP_Streaming_FHIR_Ingestion") \
        .getOrCreate()

    query = process_fhir_stream(spark, msk_brokers, topic, bronze_table)
    query.awaitTermination()
