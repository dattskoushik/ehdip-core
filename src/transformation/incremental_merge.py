import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description="Incremental Merge for Silver Iceberg Tables (CDC UPSERT)")
    parser.add_argument("--source-view", required=True, help="Temp view of new CDC data to merge")
    parser.add_argument("--target-table", required=True, help="Target Silver Iceberg table")
    parser.add_argument("--merge-key", required=True, help="Primary key for the merge condition")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("EHDIP_Incremental_Merge") \
        .getOrCreate()

    # The MERGE INTO statement for Apache Iceberg
    # This assumes 'updated_at' is used to resolve out-of-order updates
    # And 'op' holds the Debezium operation code ('c', 'u', 'd')
    merge_query = f"""
    MERGE INTO {args.target_table} t
    USING {args.source_view} s
    ON t.{args.merge_key} = s.{args.merge_key}
    WHEN MATCHED AND s.op = 'd' AND s.updated_at >= t.updated_at THEN
        DELETE
    WHEN MATCHED AND s.op IN ('u', 'c') AND s.updated_at > t.updated_at THEN
        UPDATE SET *
    WHEN NOT MATCHED AND s.op IN ('c', 'u') THEN
        INSERT *
    """

    # Execute the merge
    spark.sql(merge_query)

    print(f"Incremental merge completed successfully on {args.target_table}.")

if __name__ == "__main__":
    main()
