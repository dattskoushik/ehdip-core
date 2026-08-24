from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, expr
from pyspark.sql.types import StringType, StructType, StructField
import uuid
import os

def process_fhir_stream():
    spark = SparkSession.builder \
        .appName("EHDIP_Streaming_FHIR") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()

    # Kafka MSK Configuration (SASL/SCRAM)
    kafka_brokers = "b-1.ehdip-msk.xxxxx.c4.kafka.us-east-1.amazonaws.com:9096,b-2.ehdip-msk.xxxxx.c4.kafka.us-east-1.amazonaws.com:9096"
    kafka_topic = "fhir_raw_topic"

    # Read from MSK
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.scram.ScramLoginModule required username='ehdip_user' password='{os.environ.get('EHDIP_KAFKA_PASSWORD', 'default')}';") \
        .load()

    # Transform
    processed_df = df.selectExpr("CAST(value AS STRING) as raw_payload_json") \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system_id", lit("kafka_msk_fhir_stream")) \
        .withColumn("payload_id", expr("uuid()"))

    # Write to Bronze Iceberg
    query = processed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", "s3://ehdip-datalake-bronze-checkpoint/streaming_fhir/") \
        .toTable("glue_catalog.ehdip_bronze.raw_payloads")

    query.awaitTermination()

if __name__ == "__main__":
    process_fhir_stream()
