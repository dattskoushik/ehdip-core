# src/security/deid_engine.py

import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, date_add, when
from pyspark.sql.types import StringType, DateType
import datetime
import random

def get_spark_session():
    return SparkSession.builder \
        .appName("PHI_Deidentification_Engine") \
        .getOrCreate()

# Simulated Format-Preserving Encryption (FPE) for SSN/MRN
# In production, use Protegrity, Voltage, or AWS Macie integration
import os
def fpe_encrypt(value, secret_key=None):
    if secret_key is None:
        secret_key = os.environ.get("HIPAA_SECRET_KEY", "")
    if not value:
        return None
    # A simple deterministic hash simulation for FPE
    hasher = hashlib.sha256()
    hasher.update(f"{value}_{secret_key}".encode('utf-8'))
    # Return a mocked FPE-like string (e.g., preserving length/format conceptually)
    # Here we just return a deterministic hex for demonstration
    return hasher.hexdigest()[:len(str(value))]

fpe_encrypt_udf = udf(fpe_encrypt, StringType())

# Deterministic date shifting
def get_date_shift_offset(patient_id, secret_key=None):
    if secret_key is None:
        secret_key = os.environ.get("HIPAA_SECRET_KEY", "")
    if not patient_id:
        return 0
    # Deterministic offset between -30 and +30 days
    hasher = hashlib.md5()
    hasher.update(f"{patient_id}_{secret_key}".encode('utf-8'))
    hash_int = int(hasher.hexdigest(), 16)
    return (hash_int % 61) - 30

get_date_shift_offset_udf = udf(get_date_shift_offset, StringType())

# Safe Harbor 18 free-text redaction simulation
def redact_free_text(text):
    if not text:
        return None
    # Simulate finding and redacting PHI
    # In production, NLP models (e.g., AWS Comprehend Medical) would be used
    redacted = str(text).replace("John", "[REDACTED_NAME]").replace("Doe", "[REDACTED_NAME]")
    return redacted

redact_free_text_udf = udf(redact_free_text, StringType())


def process_deid(spark, input_table, output_table):
    """
    Applies de-identification rules to a DataFrame.
    Assumes standard columns exist for demonstration: patient_id, ssn, mrn, birth_date, clinical_notes
    """
    df_raw = spark.table(input_table)

    # Check if necessary columns exist before applying (robustness)
    cols = df_raw.columns

    df_deid = df_raw

    if 'patient_id' in cols:
        # Calculate date shift offset based on patient ID
        df_deid = df_deid.withColumn("date_shift_offset", get_date_shift_offset_udf(col("patient_id")).cast("int"))

        if 'birth_date' in cols:
             df_deid = df_deid.withColumn("birth_date",
                                          when(col("birth_date").isNotNull(),
                                               date_add(col("birth_date"), col("date_shift_offset")))
                                          .otherwise(col("birth_date")))
        if 'ssn' in cols:
             df_deid = df_deid.withColumn("ssn", fpe_encrypt_udf(col("ssn")))

        if 'mrn' in cols:
             df_deid = df_deid.withColumn("mrn", fpe_encrypt_udf(col("mrn")))

        if 'clinical_notes' in cols:
             df_deid = df_deid.withColumn("clinical_notes", redact_free_text_udf(col("clinical_notes")))

        # Drop the temporary offset column
        df_deid = df_deid.drop("date_shift_offset")

    # Write to De-identified Silver table
    df_deid.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(output_table)

if __name__ == "__main__":
    spark = get_spark_session()

    # Assume a parsed Bronze or raw Silver table with extracted fields
    INPUT_TABLE = "glue_catalog.silver.parsed_patients_raw"
    OUTPUT_TABLE = "glue_catalog.silver.deidentified_patients"

    process_deid(spark, INPUT_TABLE, OUTPUT_TABLE)
