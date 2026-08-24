from pyspark.sql import SparkSession, DataFrame

def merge_incremental_silver(spark: SparkSession, df_incremental: DataFrame, target_table_name: str, primary_key: str):
    """
    Performs an incremental MERGE INTO on Iceberg Silver tables.
    Handles out-of-order CDC updates by ensuring only newer records (`updated_at`) overwrite existing ones.
    Assumes df_incremental contains 'cdc_op' ('c', 'u', 'd') and 'updated_at' columns.
    """
    # Register incremental DataFrame as a temporary view
    df_incremental.createOrReplaceTempView("incremental_updates")

    # Construct the MERGE query
    # If cdc_op == 'd', we DELETE
    # If cdc_op == 'u' or 'c', we UPDATE/INSERT based on updated_at
    merge_query = f"""
    MERGE INTO {target_table_name} t
    USING (
        -- Handle potential duplicates within the micro-batch itself by selecting the latest
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY {primary_key} ORDER BY updated_at DESC) as rn
            FROM incremental_updates
        ) WHERE rn = 1
    ) s
    ON t.{primary_key} = s.{primary_key}
    WHEN MATCHED AND s.cdc_op = 'd' THEN DELETE
    WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
    WHEN NOT MATCHED AND s.cdc_op != 'd' THEN INSERT *
    """

    spark.sql(merge_query)
    print(f"Successfully merged incremental data into {target_table_name}")

if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession

    if len(sys.argv) != 4:
        print("Usage: incremental_merge.py <incremental_input> <target_table> <primary_key>")
        sys.exit(1)

    incremental_input = sys.argv[1]
    target_table = sys.argv[2]
    primary_key = sys.argv[3]

    spark = SparkSession.builder.appName("EHDIP_Incremental_Merge").getOrCreate()

    df_inc = spark.read.format("iceberg").load(incremental_input)
    merge_incremental_silver(spark, df_inc, target_table, primary_key)
