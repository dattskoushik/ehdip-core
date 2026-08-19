# src/transformation/omop_silver_transformer.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, coalesce

def get_spark_session():
    return SparkSession.builder \
        .appName("OMOP_Silver_Transformation") \
        .getOrCreate()

def transform_to_omop_condition(spark, input_table, vocab_table, output_table):
    """
    Transforms de-identified events to OMOP CDM v5.4 condition_occurrence table.
    """
    df_deid = spark.table(input_table)
    df_vocab = spark.table(vocab_table)

    # Example Transformation Logic for Condition Occurrence
    # Assuming df_deid has: patient_id, encounter_id, condition_code, condition_date
    # Assuming df_vocab has: concept_code, concept_id (where domain_id = 'Condition')

    # 1. Join with vocabulary to get concept_id
    df_mapped = df_deid.join(
        df_vocab,
        df_deid.condition_code == df_vocab.concept_code,
        "left"
    )

    # 2. Map to OMOP schema
    df_omop_condition = df_mapped.select(
        # OMOP usually generates a surrogate key here, we simulate it with an expression in practice
        # For PySpark, monotonically_increasing_id() can be used or a UUID cast
        col("patient_id").alias("person_id"), # In a real scenario, mapped to OMOP person_id integer
        coalesce(col("concept_id"), lit(0)).alias("condition_concept_id"), # 0 for unmapped
        col("condition_date").alias("condition_start_date"),
        col("condition_date").alias("condition_start_datetime"),
        lit(None).cast("date").alias("condition_end_date"),
        lit(None).cast("timestamp").alias("condition_end_datetime"),
        lit(32020).alias("condition_type_concept_id"), # 32020: EHR encounter diagnosis
        lit(None).cast("string").alias("stop_reason"),
        lit(None).cast("int").alias("provider_id"),
        col("encounter_id").alias("visit_occurrence_id"),
        lit(None).cast("int").alias("visit_detail_id"),
        col("condition_code").alias("condition_source_value"),
        coalesce(col("concept_id"), lit(0)).alias("condition_source_concept_id"),
        lit(None).cast("string").alias("condition_status_source_value"),
        lit(None).cast("int").alias("condition_status_concept_id")
    ).withColumn("created_at", current_timestamp())

    # 3. Write to Silver Iceberg Table
    df_omop_condition.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(output_table)

if __name__ == "__main__":
    spark = get_spark_session()

    INPUT_TABLE = "glue_catalog.silver.deidentified_clinical_events"
    VOCAB_TABLE = "glue_catalog.reference.athena_concept"
    OUTPUT_TABLE = "glue_catalog.silver.omop_condition_occurrence"

    transform_to_omop_condition(spark, INPUT_TABLE, VOCAB_TABLE, OUTPUT_TABLE)
