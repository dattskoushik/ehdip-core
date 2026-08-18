from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, get_json_object, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

def process_bronze_to_silver(spark: SparkSession, bronze_table: str, silver_table_prefix: str):
    """
    Transforms raw FHIR/HL7/X12 JSON from Bronze into Silver OMOP CDM v5.4 tables.
    Includes DeID and Vocabulary mapping stubs.
    """
    # Import the DeID engine
    import sys, os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'security'))
    from deid_engine import apply_deid_rules

    bronze_df = spark.read.table(bronze_table)

    # Extract fields from JSON payload assuming FHIR-like structure for demonstration
    extracted_df = bronze_df.select(
        col("payload_id"),
        get_json_object(col("raw_payload_json"), "$.id").alias("mrn"),
        get_json_object(col("raw_payload_json"), "$.birthDate").alias("birth_date"),
        get_json_object(col("raw_payload_json"), "$.gender").alias("gender"),
        get_json_object(col("raw_payload_json"), "$.resourceType").alias("resource_type")
    ).filter(col("resource_type") == "Patient")

    # Apply De-ID Rules before persisting to Silver
    deid_df = apply_deid_rules(extracted_df)

    # Map to OMOP PERSON table
    person_omop_df = deid_df.select(
        expr("abs(hash(payload_id))").alias("person_id"),
        expr("case when gender = 'male' then 8507 when gender = 'female' then 8532 else 0 end").alias("gender_concept_id"),
        expr("year(birth_date)").alias("year_of_birth"),
        expr("month(birth_date)").alias("month_of_birth"),
        expr("day(birth_date)").alias("day_of_birth"),
        col("mrn").alias("person_source_value"),
        current_timestamp().alias("silver_updated_at")
    )

    # Write to Silver OMOP Person table
    person_omop_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(f"{silver_table_prefix}person")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("EHDIP_OMOP_Silver_Transformer").getOrCreate()
    process_bronze_to_silver(spark, "ehdip.ehdip_bronze.raw_payloads", "ehdip.ehdip_silver.")
