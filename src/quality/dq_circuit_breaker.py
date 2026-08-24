from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

def validate_and_route(spark: SparkSession, df: DataFrame, valid_table_path: str, dlq_table_path: str):
    """
    Validation module using PyDeequ concepts (simulated here with native Spark for robustness without complex PyDeequ JVM setups)
    for structural rules and clinical plausibility with DLQ routing.

    Rules applied:
    - `condition_concept_id` is not null (Completeness)
    - `condition_start_date` <= current_date (Plausibility)
    - `person_id` is not null (Completeness)
    """
    # Create validation rules flags
    df_validated = df.withColumn(
        "is_valid_concept", F.col("condition_concept_id").isNotNull()
    ).withColumn(
        "is_valid_date", (F.col("condition_start_date") <= F.current_date())
    ).withColumn(
        "is_valid_person", F.col("person_id").isNotNull()
    )

    # Determine overall validity
    df_validated = df_validated.withColumn(
        "is_valid_record",
        F.col("is_valid_concept") & F.col("is_valid_date") & F.col("is_valid_person")
    )

    # Route valid records to Silver
    df_valid = df_validated.filter(F.col("is_valid_record") == True).drop(
        "is_valid_concept", "is_valid_date", "is_valid_person", "is_valid_record"
    )
    df_valid.write.format("iceberg").mode("append").save(valid_table_path)

    # Route invalid records to DLQ (Quarantine)
    df_invalid = df_validated.filter(F.col("is_valid_record") == False)
    # In practice, append a JSON manifest of why it failed.
    df_invalid = df_invalid.withColumn(
        "error_manifest",
        F.to_json(F.struct(
            F.col("is_valid_concept"),
            F.col("is_valid_date"),
            F.col("is_valid_person")
        ))
    )
    df_invalid.write.format("iceberg").mode("append").save(dlq_table_path)

    print(f"Validation complete. Valid records routed to {valid_table_path}, invalid to {dlq_table_path}")

if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession

    if len(sys.argv) != 4:
        print("Usage: dq_circuit_breaker.py <input_path> <valid_path> <dlq_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    valid_path = sys.argv[2]
    dlq_path = sys.argv[3]

    spark = SparkSession.builder.appName("EHDIP_DQ_Circuit_Breaker").getOrCreate()

    df = spark.read.format("iceberg").load(input_path)
    validate_and_route(spark, df, valid_path, dlq_path)
