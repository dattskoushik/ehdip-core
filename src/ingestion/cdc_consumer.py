import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col

def process_cdc(spark, input_path, iceberg_table):
    """
    Processes Debezium CDC change logs.
    Filters operations and merges to Bronze Iceberg.
    """
    # Read CDC JSON files
    df = spark.read.json(input_path)

    # Filter for standard CDC operations
    cdc_filtered_df = df.filter(col("_cdc_op").isin('c', 'u', 'd'))

    bronze_df = cdc_filtered_df.select(
        current_timestamp().alias("ingestion_timestamp"),
        lit("DEBEZIUM_CDC").alias("source_system_id"),
        col("payload").cast("string").alias("raw_payload_json") # simplified
    )

    bronze_df.createOrReplaceTempView("cdc_updates")

    # Enforce idempotent writes by registering DataFrames as temporary views and executing MERGE INTO
    merge_query = f"""
    MERGE INTO {iceberg_table} t
    USING cdc_updates s
    ON t.raw_payload_json = s.raw_payload_json
    WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_query)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Debezium CDC Ingestion")
    parser.add_argument("--input-path", required=True, help="S3 path to Debezium CDC JSON files")
    parser.add_argument("--iceberg-table", required=True, help="Target Iceberg table")

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("EHDIP_Batch_CDC_Ingestion") \
        .getOrCreate()

    process_cdc(spark, args.input_path, args.iceberg_table)
