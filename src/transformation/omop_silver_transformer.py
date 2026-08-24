from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DateType, StringType, LongType

def transform_to_omop_condition_occurrence(df_deid_events: DataFrame, df_concept_vocab: DataFrame) -> DataFrame:
    """
    Transforms de-identified bronze event payloads to Silver OMOP condition_occurrence table.
    Assumes df_deid_events contains extracted fields from JSON like:
      - person_id (masked MRN/SSN linkage)
      - condition_concept_code (ICD-10 code)
      - condition_start_date (shifted date)
    """
    # OMOP condition_occurrence schema mapping
    # We join with Athena vocabs (df_concept_vocab) to get standard concept_ids
    df_omop = df_deid_events.alias("events").join(
        df_concept_vocab.alias("vocab"),
        (F.col("events.condition_concept_code") == F.col("vocab.concept_code")) &
        (F.col("vocab.vocabulary_id") == "ICD10CM"),
        "left_outer"
    )

    df_condition = df_omop.select(
        # surrogate key - typically generated via sequence or hash
        F.abs(F.hash(F.col("events.payload_id"))).cast(LongType()).alias("condition_occurrence_id"),
        F.col("events.person_id").cast(LongType()).alias("person_id"),
        F.coalesce(F.col("vocab.concept_id"), F.lit(0)).cast(IntegerType()).alias("condition_concept_id"),
        F.col("events.condition_start_date").cast(DateType()).alias("condition_start_date"),
        F.lit(None).cast(DateType()).alias("condition_end_date"), # Simplified for example
        F.lit(32020).cast(IntegerType()).alias("condition_type_concept_id"), # EHR encounter diagnosis
        F.col("events.condition_concept_code").cast(StringType()).alias("condition_source_value"),
        F.col("vocab.concept_id").cast(IntegerType()).alias("condition_source_concept_id")
    )

    return df_condition

def transform_to_omop_drug_exposure(df_deid_events: DataFrame, df_concept_vocab: DataFrame) -> DataFrame:
    """
    Similar logic for drug_exposure mapping.
    """
    df_omop = df_deid_events.alias("events").join(
        df_concept_vocab.alias("vocab"),
        (F.col("events.drug_concept_code") == F.col("vocab.concept_code")) &
        (F.col("vocab.vocabulary_id") == "RxNorm"),
        "left_outer"
    )

    df_drug = df_omop.select(
        F.abs(F.hash(F.col("events.payload_id"))).cast(LongType()).alias("drug_exposure_id"),
        F.col("events.person_id").cast(LongType()).alias("person_id"),
        F.coalesce(F.col("vocab.concept_id"), F.lit(0)).cast(IntegerType()).alias("drug_concept_id"),
        F.col("events.drug_start_date").cast(DateType()).alias("drug_exposure_start_date"),
        F.lit(38000177).cast(IntegerType()).alias("drug_type_concept_id"), # Prescription written
        F.col("events.drug_concept_code").cast(StringType()).alias("drug_source_value")
    )

    return df_drug

if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession

    if len(sys.argv) != 5:
        print("Usage: omop_silver_transformer.py <input_deid_path> <vocab_path> <condition_output> <drug_output>")
        sys.exit(1)

    input_deid = sys.argv[1]
    vocab_path = sys.argv[2]
    cond_out = sys.argv[3]
    drug_out = sys.argv[4]

    spark = SparkSession.builder.appName("EHDIP_OMOP_Transformer").getOrCreate()

    df_deid = spark.read.format("iceberg").load(input_deid)
    df_vocab = spark.read.format("iceberg").load(vocab_path) # or parquet

    df_condition = transform_to_omop_condition_occurrence(df_deid, df_vocab)
    df_drug = transform_to_omop_drug_exposure(df_deid, df_vocab)

    df_condition.write.format("iceberg").mode("append").save(cond_out)
    df_drug.write.format("iceberg").mode("append").save(drug_out)
    print("OMOP Transformation completed.")
