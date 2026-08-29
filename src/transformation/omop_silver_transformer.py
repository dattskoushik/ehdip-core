import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

def transform_to_omop(spark, input_table, output_condition_table):
    """
    Transforms de-identified bronze payload into Silver OMOP CDM v5.4.
    Simulates mapping standard FHIR/HL7 to OMOP.
    """

    df = spark.table(input_table)

    # In a full implementation, you'd parse JSON and join with Athena vocabularies
    # For now, we simulate structural transformation to OMOP condition_occurrence

    omop_condition_df = df.select(
        col("payload_id").alias("condition_occurrence_id"),
        lit("person_123").alias("person_id"), # In practice derived from mapped patient IDs
        lit(31967).alias("condition_concept_id"), # e.g., Nausea (SNOMED mapped)
        current_timestamp().alias("condition_start_date"),
        current_timestamp().alias("condition_start_datetime"),
        current_timestamp().alias("condition_end_date"),
        current_timestamp().alias("condition_end_datetime"),
        lit(32020).alias("condition_type_concept_id"), # EHR encounter diagnosis
        lit("Nausea").alias("condition_source_value"),
        lit(0).alias("condition_source_concept_id")
    )

    # Register as temp view for idempotent MERGE
    omop_condition_df.createOrReplaceTempView("silver_updates")

    merge_query = f"""
    MERGE INTO {output_condition_table} t
    USING silver_updates s
    ON t.condition_occurrence_id = s.condition_occurrence_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_query)
    print("Silver OMOP transformation complete.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="OMOP Silver Transformer")
    parser.add_argument("--input-table", required=True, help="Input de-id Bronze table")
    parser.add_argument("--output-condition-table", required=True, help="Output Silver OMOP Condition table")

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("EHDIP_OMOP_Silver_Transformer") \
        .getOrCreate()

    transform_to_omop(spark, args.input_table, args.output_condition_table)
