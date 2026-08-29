import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col

def parse_x12(spark, input_path, iceberg_table):
    """
    Parses X12 EDI batch files (e.g., 837/835) into Parquet format
    and appends to the Bronze Iceberg table.
    """
    # Assuming text line reading for raw EDI segments for simplicity.
    # In practice, an EDI parser library or UDF would be used to structurize the payloads.
    df = spark.read.text(input_path)

    bronze_df = df.select(
        current_timestamp().alias("ingestion_timestamp"),
        lit("X12_BATCH").alias("source_system_id"),
        # Use an MD5 hash of the row as a mock payload ID
        col("value").alias("raw_payload_json")
    )

    bronze_df.createOrReplaceTempView("bronze_updates")

    # Enforce idempotent writes by registering DataFrames as temporary views and executing MERGE INTO
    merge_query = f"""
    MERGE INTO {iceberg_table} t
    USING bronze_updates s
    ON t.raw_payload_json = s.raw_payload_json
    WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_query)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Batch X12 EDI Ingestion")
    parser.add_argument("--input-path", required=True, help="S3 path to raw X12 files")
    parser.add_argument("--iceberg-table", required=True, help="Target Iceberg table")

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("EHDIP_Batch_X12_Ingestion") \
        .getOrCreate()

    parse_x12(spark, args.input_path, args.iceberg_table)
