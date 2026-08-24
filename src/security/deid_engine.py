from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sha2, date_add, date_sub, lit, regexp_replace, concat, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import random
import os

def apply_fpe_hash(df: DataFrame, column_name: str) -> DataFrame:
    """Mock Format-Preserving Encryption via Salted SHA-256 for MRN/SSN."""
    salt = os.environ.get("EHDIP_HASH_SALT", "default_salt")
    return df.withColumn(column_name, sha2(concat(col(column_name).cast("string"), lit(salt)), 256))

def apply_date_shift(df: DataFrame, date_column: str, shift_days: int) -> DataFrame:
    """Deterministic date shifting (+/- days) for de-identification."""
    if shift_days > 0:
        return df.withColumn(date_column, date_add(col(date_column), shift_days))
    else:
        return df.withColumn(date_column, date_sub(col(date_column), abs(shift_days)))

def apply_safe_harbor_text_redaction(df: DataFrame, text_column: str) -> DataFrame:
    """Redact identifiable patterns (e.g., SSN, phone numbers) from free text."""
    # Simple regex redaction for demonstration (SSN pattern)
    return df.withColumn(text_column, regexp_replace(col(text_column), r"\d{3}-\d{2}-\d{4}", "[REDACTED_SSN]"))

def deidentify_dataframe(df: DataFrame, config: dict) -> DataFrame:
    """Applies a suite of de-identification techniques based on config."""
    deid_df = df

    # Apply FPE
    for col_name in config.get("fpe_columns", []):
        if col_name in deid_df.columns:
            deid_df = apply_fpe_hash(deid_df, col_name)

    # Apply Date Shift
    shift_val = config.get("date_shift_days", -30) # Default to -30 days
    for col_name in config.get("date_columns", []):
        if col_name in deid_df.columns:
            deid_df = apply_date_shift(deid_df, col_name, shift_val)

    # Apply Text Redaction
    for col_name in config.get("free_text_columns", []):
        if col_name in deid_df.columns:
            deid_df = apply_safe_harbor_text_redaction(deid_df, col_name)

    return deid_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("EHDIP_DeId_Engine") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()

    # Logic to load Bronze data, extract JSON to columns, apply de-id, and write to an intermediate zone
    # In a real implementation this would parse raw_payload_json and apply transformations
    df = spark.read.table("glue_catalog.ehdip_bronze.raw_payloads")

    # Flatten the raw_payload_json so downstream can access columns like patient_id
    payload_schema = StructType([
        StructField("patient_id", StringType()),
        StructField("encounter_id", StringType()),
        StructField("condition_concept_id", IntegerType()),
        StructField("condition_source_value", StringType()),
        StructField("condition_source_concept_id", IntegerType()),
        StructField("encounter_date", DateType()),
        StructField("ssn", StringType()),
        StructField("clinical_notes", StringType())
    ])

    parsed_df = df.withColumn("parsed", from_json(col("raw_payload_json"), payload_schema)) \
        .select("ingestion_timestamp", "source_system_id", "payload_id", "parsed.*")

    config = {
        "fpe_columns": ["ssn"],
        "date_shift_days": -30,
        "date_columns": ["encounter_date"],
        "free_text_columns": ["clinical_notes"]
    }

    deid_df = deidentify_dataframe(parsed_df, config)
    deid_df.write.format("iceberg").mode("append").saveAsTable("glue_catalog.ehdip_bronze.deidentified_payloads")
    print("De-id engine completed.")
