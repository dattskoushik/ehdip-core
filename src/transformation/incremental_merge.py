import argparse
from pyspark.sql import SparkSession

def merge_incremental(spark, input_table, target_table):
    # For Apache Iceberg in PySpark, we enforce idempotent writes by using SQL MERGE INTO
    # Register dataframes as temp views

    # Target table is assumed to be an Iceberg table accessible via catalog

    # Read incoming incremental updates
    updates_df = spark.read.format("iceberg").load(input_table)
    updates_df.createOrReplaceTempView("updates")

    merge_sql = f"""
    MERGE INTO {target_table} t
    USING updates s
    ON t.id = s.id
    WHEN MATCHED AND s.op = 'd' AND s.updated_at > t.updated_at THEN
        DELETE
    WHEN MATCHED AND s.updated_at > t.updated_at THEN
        UPDATE SET *
    WHEN NOT MATCHED AND s.op != 'd' THEN
        INSERT *
    """

    spark.sql(merge_sql)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_table', required=True)
    parser.add_argument('--target_table', required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Incremental Merge UPSERT").getOrCreate()
    merge_incremental(spark, args.input_table, args.target_table)
