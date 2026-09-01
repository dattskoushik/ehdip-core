import argparse
from pyspark.sql import SparkSession

def main(args):
    # Enable Iceberg extensions for Spark SQL
    spark = SparkSession.builder \
        .appName("EHDIP_Incremental_Merge") \
        .getOrCreate()

    # Read incremental valid batch (from De-id or OMOP transform)
    incremental_df = spark.read \
        .format("iceberg") \
        .load(f"{args.catalog}.{args.database}.{args.source_table}")

    # Register DataFrame as a temporary view to use in MERGE INTO SQL
    incremental_df.createOrReplaceTempView("updates")

    # Perform Idempotent MERGE INTO
    # Assuming 'condition_occurrence_id' is the primary key and 'updated_at' resolves out-of-order CDC
    # Memory context: For Apache Iceberg tables in PySpark, enforce idempotent writes by registering DataFrames as temporary views and executing MERGE INTO SQL queries instead of using .mode('append').

    merge_query = f"""
    MERGE INTO {args.catalog}.{args.target_database}.{args.target_table} t
    USING updates s
    ON t.condition_occurrence_id = s.condition_occurrence_id
    WHEN MATCHED AND s.cdc_op = 'd' AND s.updated_at > t.updated_at THEN
        DELETE
    WHEN MATCHED AND (s.cdc_op = 'u' OR s.cdc_op IS NULL) AND s.updated_at > t.updated_at THEN
        UPDATE SET *
    WHEN NOT MATCHED AND (s.cdc_op = 'c' OR s.cdc_op = 'u' OR s.cdc_op IS NULL) THEN
        INSERT *
    """

    spark.sql(merge_query)
    print("Incremental MERGE completed successfully.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Incremental Merge / Upsert Engine")
    parser.add_argument("--catalog", default="glue_catalog", help="Iceberg Catalog")
    parser.add_argument("--database", default="ehdip_silver_db", help="Source Database")
    parser.add_argument("--source-table", required=True, help="Source Table")
    parser.add_argument("--target-database", default="ehdip_silver_db", help="Target Database")
    parser.add_argument("--target-table", required=True, help="Target Table")

    args = parser.parse_args()
    main(args)
