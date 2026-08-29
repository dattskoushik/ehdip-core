import argparse
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, desc

def apply_incremental_merge(spark, updates_table, target_iceberg_table, primary_key, sort_key):
    """
    Applies incremental updates handling out-of-order CDC updates via `updated_at` (or sort_key).
    Performs partition-pruned MERGE INTO on Iceberg Silver tables.
    """

    updates_df = spark.table(updates_table)

    # 1. Deduplicate updates keeping only the latest per primary key
    window_spec = Window.partitionBy(primary_key).orderBy(desc(sort_key))
    latest_updates_df = updates_df.withColumn("rn", row_number().over(window_spec)) \
                                  .filter(col("rn") == 1) \
                                  .drop("rn")

    # Register as temp view for the merge query
    latest_updates_df.createOrReplaceTempView("latest_updates")

    # 2. Perform idempotent MERGE INTO
    # Assuming the target table is partitioned, Iceberg will automatically prune partitions
    merge_query = f"""
    MERGE INTO {target_iceberg_table} t
    USING latest_updates s
    ON t.{primary_key} = s.{primary_key}
    WHEN MATCHED AND t.{sort_key} < s.{sort_key} THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_query)
    print(f"Incremental merge into {target_iceberg_table} complete.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Incremental CDC Upsert Engine")
    parser.add_argument("--updates-table", required=True, help="Input table with latest CDC/batch updates")
    parser.add_argument("--target-iceberg-table", required=True, help="Target Silver Iceberg table")
    parser.add_argument("--primary-key", required=True, help="Primary key column for merge condition")
    parser.add_argument("--sort-key", required=True, help="Column to sort by for latest record (e.g., updated_at)")

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("EHDIP_Incremental_Merge") \
        .getOrCreate()

    apply_incremental_merge(spark, args.updates_table, args.target_iceberg_table, args.primary_key, args.sort_key)
