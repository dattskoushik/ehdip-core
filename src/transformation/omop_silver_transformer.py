import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, monotonically_increasing_id

def transform_to_omop(spark, input_table, db_prefix):
    # Read de-identified flat records
    df = spark.read.table(input_table)

    # Very basic mock mapping to OMOP Condition Occurrence
    # In reality, this would involve Athena vocabulary joins to map source codes to standard concept_ids
    condition_df = df.filter(col("event_type") == "diagnosis") \
        .withColumn("condition_occurrence_id", monotonically_increasing_id()) \
        .withColumn("person_id", col("patient_id")) \
        .withColumn("condition_concept_id", lit(0)) \
        .withColumn("condition_start_date", col("event_date")) \
        .withColumn("condition_source_value", col("code")) \
        .select("condition_occurrence_id", "person_id", "condition_concept_id", "condition_start_date", "condition_source_value")

    condition_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(f"{db_prefix}.condition_occurrence")

    # Drug Exposure mock mapping
    drug_df = df.filter(col("event_type") == "medication") \
        .withColumn("drug_exposure_id", monotonically_increasing_id()) \
        .withColumn("person_id", col("patient_id")) \
        .withColumn("drug_concept_id", lit(0)) \
        .withColumn("drug_exposure_start_date", col("event_date")) \
        .withColumn("drug_source_value", col("code")) \
        .select("drug_exposure_id", "person_id", "drug_concept_id", "drug_exposure_start_date", "drug_source_value")

    drug_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(f"{db_prefix}.drug_exposure")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", required=True)
    parser.add_argument("--db_prefix", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("OMOP_Silver_Transformer") \
        .getOrCreate()

    transform_to_omop(spark, args.input_table, args.db_prefix)
