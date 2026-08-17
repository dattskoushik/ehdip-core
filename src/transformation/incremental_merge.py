from pyspark.sql import SparkSession

def get_spark_session():
    return SparkSession.builder \
        .appName("EHDIP_Incremental_Merge") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue.warehouse", "s3://ehdip-data-lake-silver/") \
        .getOrCreate()

def execute_incremental_merge(spark, target_table_name, source_view_name, join_key):
    """
    Executes an idempotent MERGE INTO on Iceberg Silver tables handling out-of-order updates.
    Assumes a 'updated_at' column exists for state resolution.
    """

    merge_sql = f"""
    MERGE INTO glue.ehdip_silver.{target_table_name} t
    USING {source_view_name} s
    ON t.{join_key} = s.{join_key}
    WHEN MATCHED AND s.cdc_op = 'd' AND s.updated_at > t.updated_at THEN
        DELETE
    WHEN MATCHED AND s.updated_at > t.updated_at THEN
        UPDATE SET *
    WHEN NOT MATCHED AND s.cdc_op != 'd' THEN
        INSERT *
    """

    # In a real environment, this execute would run against the Iceberg catalog
    print(f"Executing MERGE SQL: \n{merge_sql}")
    # spark.sql(merge_sql)
    return merge_sql

if __name__ == "__main__":
    spark = get_spark_session()
    print("Incremental Merge Module Initialized.")
