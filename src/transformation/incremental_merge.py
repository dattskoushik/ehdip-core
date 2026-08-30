import argparse
from pyspark.sql import SparkSession

def merge_cdc(spark, cdc_updates_table, target_iceberg_table):
    # Enforce idempotent writes for Iceberg using MERGE INTO instead of append mode
    # Load new updates and create a temp view
    updates_df = spark.read.table(cdc_updates_table)
    updates_df.createOrReplaceTempView("updates")

    # Perform Merge
    merge_query = f"""
    MERGE INTO {target_iceberg_table} t
    USING updates u
    ON t.payload_id = u.payload_id
    WHEN MATCHED AND u.op = 'd' THEN DELETE
    WHEN MATCHED AND u.op = 'u' AND u.updated_at > t.updated_at THEN UPDATE SET *
    WHEN NOT MATCHED AND u.op IN ('c', 'u') THEN INSERT *
    """

    spark.sql(merge_query)
    print(f"Successfully merged updates into {target_iceberg_table}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--cdc_updates_table", required=True)
    parser.add_argument("--target_iceberg_table", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("CDC_Incremental_Merge") \
        .getOrCreate()

    merge_cdc(spark, args.cdc_updates_table, args.target_iceberg_table)
