from pyspark.sql import SparkSession

def perform_incremental_merge(spark: SparkSession, updates_df, target_table: str):
    """
    Performs incremental partition-pruned MERGE INTO on Iceberg Silver tables.
    Handles out-of-order CDC updates via updated_at.
    """
    # Create temporary view for the updates
    updates_df.createOrReplaceTempView("updates")

    # Construct the MERGE statement
    # Assuming standard columns: id, data, updated_at, and partition column e.g., date
    merge_sql = f"""
    MERGE INTO {target_table} t
    USING updates u
    ON t.id = u.id
    WHEN MATCHED AND u._cdc_op = 'd' THEN
        DELETE
    WHEN MATCHED AND u.updated_at > t.updated_at THEN
        UPDATE SET *
    WHEN NOT MATCHED AND u._cdc_op != 'd' THEN
        INSERT *
    """

    spark.sql(merge_sql)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Incremental_Merge") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

    # updates_df = spark.read.parquet("s3://ehdip-bronze-data-lake/cdc_updates/")
    # perform_incremental_merge(spark, updates_df, "ehdip_catalog.ehdip.silver_patients")
