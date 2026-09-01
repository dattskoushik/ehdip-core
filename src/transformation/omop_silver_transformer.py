import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

def transform_to_omop(spark, input_table, output_condition, output_drug):
    # Load standardized de-identified silver events
    df = spark.read.format("iceberg").load(input_table)

    # Mocking Athena vocabulary lookup (simplified OMOP mapping)
    # 1. Map to condition_occurrence table
    # Assuming schema: patient_id, admission_date_deid, condition_code, drug_code
    condition_df = df.filter(col("condition_code").isNotNull()) \
        .select(
            col("patient_id").alias("person_id"),
            col("condition_code").alias("condition_concept_id"),
            col("admission_date_deid").alias("condition_start_date"),
            lit("OMOP_MOCK").alias("condition_source_value")
        )

    condition_df.write.format("iceberg").mode("append").saveAsTable(output_condition)

    # 2. Map to drug_exposure table
    drug_df = df.filter(col("drug_code").isNotNull()) \
        .select(
            col("patient_id").alias("person_id"),
            col("drug_code").alias("drug_concept_id"),
            col("admission_date_deid").alias("drug_exposure_start_date"),
            lit("OMOP_MOCK").alias("drug_source_value")
        )

    drug_df.write.format("iceberg").mode("append").saveAsTable(output_drug)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_table', required=True)
    parser.add_argument('--output_condition', required=True)
    parser.add_argument('--output_drug', required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("OMOP Silver Transformer").getOrCreate()
    transform_to_omop(spark, args.input_table, args.output_condition, args.output_drug)
