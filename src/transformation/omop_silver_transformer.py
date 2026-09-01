import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def main(args):
    spark = SparkSession.builder \
        .appName("EHDIP_OMOP_Silver_Transformer") \
        .getOrCreate()

    # Read from Bronze (or De-id intermediate)
    raw_df = spark.read \
        .format("iceberg") \
        .load(f"{args.catalog}.{args.database}.{args.input_table}")

    # Standardize to OMOP CDM v5.4
    # In a real scenario, this involves complex mapping using Athena vocabularies
    # Here we simulate the structural transformation for some OMOP tables

    # Example: Condition Occurrence Table
    condition_occurrence_df = raw_df \
        .select(
            col("payload_id").alias("condition_occurrence_id"),
            col("patient_mrn_deid").alias("person_id"), # Linked person_id
            lit(0).alias("condition_concept_id"), # Requires Athena lookup
            col("admission_date_shifted").alias("condition_start_date"),
            col("admission_date_shifted").alias("condition_start_datetime"),
            lit(None).cast("date").alias("condition_end_date"),
            lit(None).cast("timestamp").alias("condition_end_datetime"),
            lit(32020).alias("condition_type_concept_id"), # EHR encounter diagnosis
            col("source_system_id").alias("condition_source_value"),
            lit(0).alias("condition_source_concept_id"),
            lit(None).cast("string").alias("condition_status_source_value"),
            lit(0).alias("condition_status_concept_id")
        )

    # Note: splink linkage logic for person_id resolution would typically happen
    # prior to or during this step to ensure all records map to a unified person_id.
    # We write to the silver zone

    condition_occurrence_df.write \
        .format("iceberg") \
        .mode("append") \
        .save(f"{args.catalog}.{args.silver_database}.{args.output_table}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Silver Standardization to OMOP CDM v5.4")
    parser.add_argument("--catalog", default="glue_catalog", help="Iceberg Catalog")
    parser.add_argument("--database", default="ehdip_bronze_db", help="Source Database")
    parser.add_argument("--input-table", required=True, help="Input Table")
    parser.add_argument("--silver-database", default="ehdip_silver_db", help="Silver Database")
    parser.add_argument("--output-table", default="condition_occurrence", help="Target table")

    args = parser.parse_args()
    main(args)
