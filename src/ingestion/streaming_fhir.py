import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, MapType

def get_spark_session(app_name="StreamingFHIR_Ingestion"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.type", "glue") \
        .getOrCreate()

def main():
    if len(sys.argv) < 3:
        print("Usage: streaming_fhir.py <kafka_bootstrap_servers> <kafka_topic>")
        sys.exit(1)

    kafka_bootstrap_servers = sys.argv[1]
    kafka_topic = sys.argv[2]

    spark = get_spark_session()

    # Define schema for incoming FHIR JSON (simplified for dynamic payload)
    # Using MapType to handle schema evolution naturally or string to hold raw JSON
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("resourceType", StringType(), True)
    ])

    # Read from Kafka/MSK
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON payload and construct audit fields
    # Keep the raw payload JSON to support schema evolution
    parsed_df = df_stream \
        .selectExpr("CAST(value AS STRING) as raw_payload_json") \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system_id", expr("'AWS_MSK_FHIR_TOPIC'")) \
        .withColumn("payload_id", expr("uuid()"))

    # Select columns matching the Bronze schema
    final_df = parsed_df.select(
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
        .option("checkpointLocation", "s3://ehdip-bronze-raw/checkpoints/fhir_ingestion/") \
        .toTable("glue_catalog.ehdip_data_lake.bronze_raw_payloads")

    query.awaitTermination()

if __name__ == "__main__":
    main()
