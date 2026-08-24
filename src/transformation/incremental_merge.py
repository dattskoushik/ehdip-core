from pyspark.sql import SparkSession, DataFrame

def merge_cdc_to_silver(spark: SparkSession, updates_df: DataFrame, target_table: str, join_key: str):
    """
    Performs an incremental MERGE INTO (UPSERT) on an Iceberg table,
    handling late-arriving CDC updates using an updated_at condition.
    """

    # Create temporary view for the microbatch updates
    updates_df.createOrReplaceTempView("updates")

    merge_sql = f"""
    MERGE INTO glue_catalog.{target_table} t
    USING updates u
    ON t.{join_key} = u.{join_key}
    WHEN MATCHED AND u.op = 'd' THEN DELETE
    WHEN MATCHED AND u.updated_at > t.updated_at THEN UPDATE SET *
    WHEN NOT MATCHED AND u.op != 'd' THEN INSERT *
    """

    spark.sql(merge_sql)
    print(f"Merge into {target_table} completed successfully.")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("EHDIP_Incremental_Merge") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()

    # Example Usage:
    df = spark.read.table("glue_catalog.ehdip_bronze.deidentified_payloads")
    merge_cdc_to_silver(spark, df, "ehdip_silver.omop_person", "person_id")
