import sys
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, udf
from pyspark.sql.types import StringType

def generate_uuid():
    return str(uuid.uuid4())

uuid_udf = udf(generate_uuid, StringType())

def parse_x12_to_bronze(spark: SparkSession, input_path: str, bronze_table: str):
    """
    Parses X12 EDI 837/835 flat files and stores them into the Bronze Iceberg table.
    For simplicity, treating each line as a raw payload.
    """
    df_raw = spark.read.text(input_path)

    df_bronze = df_raw.selectExpr("value as raw_payload_json") \
        .withColumn("payload_id", uuid_udf()) \
        .withColumn("source_system_id", lit("EDI_X12_BATCH")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .select("payload_id", "source_system_id", "ingestion_timestamp", "raw_payload_json")

    # Write to Bronze Iceberg table
    df_bronze.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(bronze_table)

    print(f"Successfully processed X12 batch from {input_path} into {bronze_table}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: x12_batch_parser.py <input_path> <bronze_table>")
        sys.exit(1)

    input_path = sys.argv[1]
    bronze_table = sys.argv[2]

    spark = SparkSession.builder \
        .appName("EHDIP_X12_Batch_Ingestion") \
        .getOrCreate()

    parse_x12_to_bronze(spark, input_path, bronze_table)
