from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, expr, from_json
from pyspark.sql.types import StructType, StructField, StringType

def process_cdc_stream(spark: SparkSession, kafka_bootstrap_servers: str, kafka_topic: str, checkpoint_location: str, output_path: str):
    """
    Processes Debezium CDC change logs.
    """
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Define minimal Debezium schema
    debezium_schema = StructType([
        StructField("payload", StructType([
            StructField("op", StringType(), True),
            StructField("after", StringType(), True)
        ]))
    ])

    parsed_df = df.selectExpr("CAST(value AS STRING) as json_val") \
        .withColumn("data", from_json(col("json_val"), debezium_schema)) \
        .select(col("data.payload.op").alias("_cdc_op"), col("data.payload.after").alias("raw_payload_json"))

    # Filter out reads, keep create, update, delete
    cdc_df = parsed_df.filter(col("_cdc_op").isin("c", "u", "d"))

    enriched_df = cdc_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system_id", lit("debezium_cdc")) \
        .withColumn("payload_id", expr("uuid()"))

    query = enriched_df \
        .writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .option("path", output_path) \
        .start()

    return query

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CDC_Consumer") \
        .getOrCreate()

    # query = process_cdc_stream(spark, "localhost:9092", "dbserver1.inventory.customers", "s3://checkpoints/cdc", "s3://ehdip-bronze-data-lake/cdc/")
    # query.awaitTermination()
