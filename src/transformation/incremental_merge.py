# src/transformation/incremental_merge.py

from pyspark.sql import SparkSession

def get_spark_session():
    return SparkSession.builder \
        .appName("Incremental_CDC_Merge") \
        .getOrCreate()

def merge_cdc_to_silver(spark, source_view, target_table):
    """
    Performs an incremental MERGE INTO on Iceberg Silver tables,
    handling out-of-order CDC updates via `updated_at`.
    """

    # In Spark 3.x with Iceberg, we can use SQL for the MERGE operation.
    # The source_view should be a temporary view of the deduplicated incoming CDC batch.

    merge_sql = f"""
    MERGE INTO {target_table} t
    USING {source_view} s
    ON t.person_id = s.person_id
    WHEN MATCHED AND s.op = 'd' AND s.updated_at > t.updated_at THEN
        DELETE
    WHEN MATCHED AND s.op IN ('u', 'c') AND s.updated_at > t.updated_at THEN
        UPDATE SET *
    WHEN NOT MATCHED AND s.op IN ('c', 'u') THEN
        INSERT *
    """

    print(f"Executing MERGE:\n{merge_sql}")
    spark.sql(merge_sql)

if __name__ == "__main__":
    spark = get_spark_session()

    # Example usage:
    # 1. Read new CDC events
    # df_cdc = spark.read.table("glue_catalog.bronze.raw_cdc_payload").filter(...)
    # 2. Parse JSON, extract fields (person_id, op, updated_at, etc.)
    # 3. Deduplicate incoming batch to get the latest state per person_id
    # 4. Create temp view
    # df_deduped.createOrReplaceTempView("new_cdc_events")

    TARGET_TABLE = "glue_catalog.silver.omop_person"

    # merge_cdc_to_silver(spark, "new_cdc_events", TARGET_TABLE)
