from pyspark.sql import SparkSession

def perform_incremental_upsert(spark: SparkSession, source_view: str, target_table: str, join_key: str):
    """
    Performs an idempotent MERGE INTO operation on an Iceberg table using Spark SQL.
    Handles out-of-order CDC updates by checking the 'updated_at' timestamp.
    """

    merge_sql = f"""
    MERGE INTO {target_table} t
    USING {source_view} s
    ON t.{join_key} = s.{join_key}
    WHEN MATCHED AND s.op = 'd' THEN
        DELETE
    WHEN MATCHED AND s.updated_at > t.updated_at THEN
        UPDATE SET *
    WHEN NOT MATCHED AND s.op != 'd' THEN
        INSERT *
    """

    # In a real pipeline, we would isolate micro-batches and create the source_view temp table here
    spark.sql(merge_sql)
    print(f"Incremental MERGE completed for {target_table}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("EHDIP_Incremental_Merge").getOrCreate()
    # Meant to be called by orchestration with specific tables
