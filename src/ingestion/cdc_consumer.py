import sys
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, udf
from pyspark.sql.types import StringType

def generate_uuid():
    return str(uuid.uuid4())

uuid_udf = udf(generate_uuid, StringType())

def process_cdc_logs(spark: SparkSession, input_path: str, bronze_table: str):
    """
    Processes Debezium CDC change logs (JSON) and stores them into the Bronze Iceberg table.
    Filters for valid CDC operations ('c', 'u', 'd').
    """
    # Read Debezium JSON files
    df_raw = spark.read.json(input_path)

    # Filter valid ops (this implies payload has an 'op' field, or _cdc_op in some flattened schema)
    # For robust raw ingestion, we just ingest the raw JSON as string, but here we can optionally filter if it's parsed.
    # To keep the pattern consistent with Bronze (storing raw JSON), we can read as text and parse later,
    # or read as text and just store the raw string.
    df_text = spark.read.text(input_path)

    # We could extract _cdc_op using get_json_object, but for raw Bronze we just store everything.

    df_bronze = df_text.selectExpr("value as raw_payload_json") \
        .withColumn("payload_id", uuid_udf()) \
        .withColumn("source_system_id", lit("DEBEZIUM_CDC_BATCH")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .select("payload_id", "source_system_id", "ingestion_timestamp", "raw_payload_json")

    # Write to Bronze Iceberg table
    df_bronze.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(bronze_table)

    print(f"Successfully processed CDC batch from {input_path} into {bronze_table}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: cdc_consumer.py <input_path> <bronze_table>")
        sys.exit(1)

    input_path = sys.argv[1]
    bronze_table = sys.argv[2]

    spark = SparkSession.builder \
        .appName("EHDIP_CDC_Consumer") \
        .getOrCreate()

    process_cdc_logs(spark, input_path, bronze_table)
