import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, lit, split, col, expr

def main():
    parser = argparse.ArgumentParser(description="Batch Ingestion for X12 EDI Claims to Bronze Iceberg")
    parser.add_argument("--input-path", required=True, help="S3 path containing raw X12 EDI files")
    parser.add_argument("--bronze-table", required=True, help="Target Bronze Iceberg table (e.g., catalog.bronze_db.raw_payloads)")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("EHDIP_X12_Batch_Parser") \
        .getOrCreate()

    # Read raw text lines from EDI files
    # X12 segment delimiter is usually ~
    # For simplicity, we treat each line as a raw payload. In practice, a full parser like 'smooks' or custom logic would rebuild the transaction.
    raw_df = spark.read.text(args.input_path)

    processed_df = raw_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system_id", lit("X12_EDI_BATCH")) \
        .withColumn("file_name", input_file_name()) \
        .withColumn("payload_id", expr("uuid()")) \
        .withColumnRenamed("value", "raw_payload_json") \
        .select(
            "ingestion_timestamp",
            "source_system_id",
            "payload_id",
            "raw_payload_json"
        )

    # Use merge for idempotency instead of blindly appending
    # We will register a temporary view and run a MERGE query
    processed_df.createOrReplaceTempView("new_payloads")

    merge_query = f"""
    MERGE INTO {args.bronze_table} t
    USING new_payloads s
    ON t.payload_id = s.payload_id
    WHEN NOT MATCHED THEN
        INSERT *
    """

    spark.sql(merge_query)

if __name__ == "__main__":
    main()
