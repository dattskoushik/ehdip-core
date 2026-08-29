import os
import hashlib
from pyspark.sql.functions import udf, col, when, date_add
from pyspark.sql.types import StringType, DateType
from pyspark.sql import SparkSession
import argparse

def get_hash_salt():
    salt = os.environ.get("EHDIP_HASH_SALT")
    if not salt:
        raise ValueError("FATAL: HIPAA Compliance Error - EHDIP_HASH_SALT environment variable is required for FPE.")
    return salt

def fpe_mask(value):
    if not value:
        return value
    salt = get_hash_salt()
    # Simple HMAC-like hashing for Format-Preserving Encryption simulation
    return hashlib.sha256((str(value) + salt).encode('utf-8')).hexdigest()[:16]

fpe_mask_udf = udf(fpe_mask, StringType())

def calculate_date_shift(patient_id):
    if not patient_id:
        return 0
    # Deterministic date-shifting based on patient ID modulo
    # e.g., hash the patient ID, convert to int, modulo 60, shift by -30 to +30 days
    salt = get_hash_salt()
    h = hashlib.sha256((str(patient_id) + salt).encode('utf-8')).hexdigest()
    shift = (int(h[:8], 16) % 61) - 30
    return shift

date_shift_udf = udf(calculate_date_shift, StringType())

def safe_harbor_redact(text):
    if not text:
        return text
    # In a real scenario, this would use an NLP library or regex for Safe Harbor 18 identifiers
    # Here we simulate by replacing potential identifiers (e.g., specific formats)
    # Simple mock: mask everything containing digits that looks like an ID
    import re
    return re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[REDACTED SSN]', text)

safe_harbor_udf = udf(safe_harbor_redact, StringType())

def apply_deid_rules(df):
    """
    Applies de-identification rules to a DataFrame containing PHI.
    Assumes columns: patient_id, ssn, mrn, dob, notes
    """

    # 1. Calculate shift per patient
    df_with_shift = df.withColumn("date_shift_days", date_shift_udf(col("patient_id")).cast("integer"))

    # 2. Apply FPE to SSN and MRN
    # 3. Shift DOB
    # 4. Apply Safe Harbor to notes
    deid_df = df_with_shift \
        .withColumn("ssn_masked", fpe_mask_udf(col("ssn"))) \
        .withColumn("mrn_masked", fpe_mask_udf(col("mrn"))) \
        .withColumn("dob_shifted", date_add(col("dob"), col("date_shift_days"))) \
        .withColumn("notes_redacted", safe_harbor_udf(col("notes"))) \
        .drop("date_shift_days", "ssn", "mrn", "dob", "notes")

    return deid_df

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="PHI De-Identification Engine")
    parser.add_argument("--input-table", required=True, help="Input Iceberg table with PHI")
    parser.add_argument("--output-table", required=True, help="Output Iceberg table for De-identified data")

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("EHDIP_DeID_Engine") \
        .getOrCreate()

    # Example execution:
    # df = spark.table(args.input_table)
    # deid_df = apply_deid_rules(df)
    # deid_df.writeTo(args.output_table).createOrReplace()
    print("De-id module configured.")
