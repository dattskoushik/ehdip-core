from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import sys

def get_spark_session(app_name="OMOP_Silver_Transformer"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.type", "glue") \
        .getOrCreate()

def transform_to_omop(spark, input_table, output_database):
    """
    Transforms de-identified bronze payload to OMOP CDM v5.4 structure.
    Simplified version focusing on structural mapping.
    """
    df_bronze = spark.read.table(input_table)

    # 1. Map to condition_occurrence
    # Assuming json parsing has already extracted relevant fields into columns
    # like 'patient_id', 'condition_concept_id', 'condition_start_date'
    if "condition_concept_id" in df_bronze.columns:
        condition_df = df_bronze.select(
            col("payload_id").alias("condition_occurrence_id"),
            col("patient_id").alias("person_id"),
            col("condition_concept_id"),
            col("condition_start_date"),
            col("condition_end_date"),
            lit(32020).alias("condition_type_concept_id") # EHR
        )
        condition_df.write.format("iceberg").mode("append").saveAsTable(f"{output_database}.condition_occurrence")

    # 2. Map to drug_exposure
    if "drug_concept_id" in df_bronze.columns:
        drug_df = df_bronze.select(
            col("payload_id").alias("drug_exposure_id"),
            col("patient_id").alias("person_id"),
            col("drug_concept_id"),
            col("drug_exposure_start_date"),
            col("drug_exposure_end_date"),
            lit(38000177).alias("drug_type_concept_id") # Prescription written
        )
        drug_df.write.format("iceberg").mode("append").saveAsTable(f"{output_database}.drug_exposure")

    print(f"Successfully transformed and loaded OMOP tables to {output_database}")

def main():
    if len(sys.argv) < 3:
        print("Usage: omop_silver_transformer.py <input_bronze_table> <output_silver_db>")
        sys.exit(1)

    input_table = sys.argv[1]
    output_database = sys.argv[2]

    spark = get_spark_session()
    transform_to_omop(spark, input_table, output_database)

if __name__ == "__main__":
    main()
