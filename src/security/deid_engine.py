from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, date_add, udf, regexp_replace, expr
from pyspark.sql.types import StringType
import re
import sys

def get_spark_session(app_name="PHI_DeID_Engine"):
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

# Simulated Format-Preserving Encryption (FPE) using SHA-256 for demonstration
# In reality, you would use a dedicated FPE library that maintains format (e.g. NIST FF1/FF3)
def simulate_fpe(value):
    if not value: return value
    import hashlib
    # Simple hash for demo
    return hashlib.sha256(value.encode('utf-8')).hexdigest()[:len(value)]

fpe_udf = udf(simulate_fpe, StringType())

def safe_harbor_redact(text_col):
    # Regex to redact potential phone numbers or SSNs from free text (Safe Harbor 18)
    pattern = r'\b(\d{3}-\d{2}-\d{4}|\d{3}-\d{3}-\d{4})\b'
    return regexp_replace(text_col, pattern, '[REDACTED]')

def deterministic_shift_days(patient_id):
    if not patient_id: return 0
    import hashlib
    # Modulo arithmetic to generate a shift between -30 and 30 days based on patient_id
    hash_val = int(hashlib.md5(patient_id.encode('utf-8')).hexdigest(), 16)
    return (hash_val % 61) - 30

shift_udf = udf(deterministic_shift_days, StringType())

def apply_deid_rules(df, patient_id_col, ssn_col, mrn_col, dob_col, notes_col):
    """
    Applies de-identification rules:
    - FPE on SSN and MRN
    - Deterministic date-shifting (+/- 30 days) on DOB based on Patient ID
    - Safe Harbor 18 redaction on Notes
    """
    if ssn_col in df.columns:
        df = df.withColumn(f"{ssn_col}_deid", fpe_udf(col(ssn_col)))

    if mrn_col in df.columns:
        df = df.withColumn(f"{mrn_col}_deid", fpe_udf(col(mrn_col)))

    if dob_col in df.columns and patient_id_col in df.columns:
        # Deterministic Date shift: +/- 30 days based on patient ID
        df = df.withColumn("shift_days", shift_udf(col(patient_id_col)).cast("int"))
        df = df.withColumn(f"{dob_col}_deid", expr(f"date_add({dob_col}, shift_days)"))
        df = df.drop("shift_days")

    if notes_col in df.columns:
        df = df.withColumn(f"{notes_col}_deid", safe_harbor_redact(col(notes_col)))

    return df

def main():
    if len(sys.argv) < 3:
        print("Usage: deid_engine.py <input_table> <output_table>")
        sys.exit(1)

    input_table = sys.argv[1]
    output_table = sys.argv[2]

    spark = get_spark_session()

    df = spark.read.table(input_table)

    # Assume schema has patient_id, ssn, mrn, dob, clinical_notes columns
    df_deid = apply_deid_rules(df, "patient_id", "ssn", "mrn", "dob", "clinical_notes")

    # Save the de-identified dataframe
    df_deid.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(output_table)

    print(f"De-identification complete. Output written to {output_table}")

if __name__ == "__main__":
    main()
