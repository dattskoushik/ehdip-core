from pyspark.sql import SparkSession
import sys

def get_spark_session(app_name="Incremental_CDC_Merge"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.type", "glue") \
        .getOrCreate()

def merge_cdc(spark, cdc_view, target_table):
    """
    Performs an incremental MERGE INTO target_table using cdc_view data.
    Assumes cdc_view has _cdc_op ('c', 'u', 'd') and updated_at to resolve out-of-order events.
    """
    merge_sql = f"""
    MERGE INTO {target_table} t
    USING {cdc_view} s
    ON t.payload_id = s.payload_id
    WHEN MATCHED AND s._cdc_op = 'd' AND s.updated_at >= t.updated_at THEN
        DELETE
    WHEN MATCHED AND s._cdc_op IN ('c', 'u') AND s.updated_at >= t.updated_at THEN
        UPDATE SET *
    WHEN NOT MATCHED AND s._cdc_op IN ('c', 'u') THEN
        INSERT *
    """

    spark.sql(merge_sql)
    print(f"Merge operation completed on {target_table}")

def main():
    if len(sys.argv) < 3:
        print("Usage: incremental_merge.py <cdc_temp_view> <target_iceberg_table>")
        sys.exit(1)

    cdc_view = sys.argv[1]
    target_table = sys.argv[2]

    spark = get_spark_session()
    merge_cdc(spark, cdc_view, target_table)

if __name__ == "__main__":
    main()
