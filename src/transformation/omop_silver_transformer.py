import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

def main():
    parser = argparse.ArgumentParser(description="OMOP CDM v5.4 Silver Transformer")
    parser.add_argument("--input-table", required=True, help="Input De-Identified table")
    parser.add_argument("--silver-db", required=True, help="Target Silver Database")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("EHDIP_OMOP_Silver_Transformer") \
        .getOrCreate()

    input_df = spark.table(args.input_table)

    # ---------------------------------------------------------
    # 1. Transform to OMOP person
    # ---------------------------------------------------------
    if "patient_id" in input_df.columns:
        # Assuming we have basic demographic data available in the input
        # Note: mapping logic requires an OMOP vocabulary mapping usually, this is a simplified stub.
        person_df = input_df.select(
            col("patient_id").alias("person_source_value"),
            col("birth_date_shifted").alias("birth_datetime") if "birth_date_shifted" in input_df.columns else lit(None).cast("timestamp").alias("birth_datetime"),
            # Placeholder for other OMOP fields
            lit(0).alias("gender_concept_id"),
            lit(0).alias("race_concept_id"),
            lit(0).alias("ethnicity_concept_id")
        ).dropDuplicates(["person_source_value"])

        # Idempotent write using dynamic partition overwrite or overwrite mode for the whole table (depending on table setup)
        # Using MERGE or insert overwrite. Since person table doesn't have an explicit partition in this script,
        # we will use createOrReplaceTempView and MERGE.
        person_df.createOrReplaceTempView("new_person")
        spark.sql(f"""
        MERGE INTO {args.silver_db}.person t
        USING new_person s
        ON t.person_source_value = s.person_source_value
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """)

    # ---------------------------------------------------------
    # 2. Transform to OMOP condition_occurrence
    # ---------------------------------------------------------
    if "condition_code" in input_df.columns:
        # Using Athena vocabularies conceptually mapped here
        condition_df = input_df.filter(col("condition_code").isNotNull()).select(
            col("patient_id").alias("person_source_value"), # We would join to person table to get actual person_id later
            col("condition_code").alias("condition_source_value"),
            lit(0).alias("condition_concept_id"), # In reality, mapped via Athena vocabularies (e.g. concept table)
            col("condition_start_date_shifted").alias("condition_start_date") if "condition_start_date_shifted" in input_df.columns else lit(None).cast("date").alias("condition_start_date"),
            lit(32020).alias("condition_type_concept_id") # e.g. EHR encounter diagnosis
        )
        # Assuming no strict PK for condition_occurrence, we can use dynamic partition overwrite if partitioned by start_date,
        # or simple MERGE using all columns to prevent duplicates.
        condition_df.createOrReplaceTempView("new_condition")
        spark.sql(f"""
        MERGE INTO {args.silver_db}.condition_occurrence t
        USING new_condition s
        ON t.person_source_value = s.person_source_value
           AND t.condition_source_value = s.condition_source_value
           AND t.condition_start_date = s.condition_start_date
        WHEN NOT MATCHED THEN
            INSERT *
        """)

    # ---------------------------------------------------------
    # 3. Transform to OMOP drug_exposure
    # ---------------------------------------------------------
    if "drug_code" in input_df.columns:
        drug_df = input_df.filter(col("drug_code").isNotNull()).select(
            col("patient_id").alias("person_source_value"),
            col("drug_code").alias("drug_source_value"),
            lit(0).alias("drug_concept_id"),
            col("drug_exposure_start_date_shifted").alias("drug_exposure_start_date") if "drug_exposure_start_date_shifted" in input_df.columns else lit(None).cast("date").alias("drug_exposure_start_date"),
            lit(38000177).alias("drug_type_concept_id") # e.g. Prescription written
        )
        drug_df.createOrReplaceTempView("new_drug")
        spark.sql(f"""
        MERGE INTO {args.silver_db}.drug_exposure t
        USING new_drug s
        ON t.person_source_value = s.person_source_value
           AND t.drug_source_value = s.drug_source_value
           AND t.drug_exposure_start_date = s.drug_exposure_start_date
        WHEN NOT MATCHED THEN
            INSERT *
        """)

    # ---------------------------------------------------------
    # 4. Transform to OMOP measurement
    # ---------------------------------------------------------
    if "measurement_code" in input_df.columns:
        measurement_df = input_df.filter(col("measurement_code").isNotNull()).select(
            col("patient_id").alias("person_source_value"),
            col("measurement_code").alias("measurement_source_value"),
            lit(0).alias("measurement_concept_id"),
            col("measurement_date_shifted").alias("measurement_date") if "measurement_date_shifted" in input_df.columns else lit(None).cast("date").alias("measurement_date"),
            lit(44818701).alias("measurement_type_concept_id") # e.g. From physical examination
        )
        measurement_df.createOrReplaceTempView("new_measurement")
        spark.sql(f"""
        MERGE INTO {args.silver_db}.measurement t
        USING new_measurement s
        ON t.person_source_value = s.person_source_value
           AND t.measurement_source_value = s.measurement_source_value
           AND t.measurement_date = s.measurement_date
        WHEN NOT MATCHED THEN
            INSERT *
        """)

if __name__ == "__main__":
    main()
