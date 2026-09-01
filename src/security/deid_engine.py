import os
import argparse
import hashlib
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, regexp_replace
from pyspark.sql.types import StringType, DateType

# Memory states: "Format-Preserving Encryption (FPE) for PHI uses salted hashes sourced from environment variables (e.g., EHDIP_HASH_SALT) rather than unsalted hashing."
salt = os.environ.get("EHDIP_HASH_SALT", "default_salt")

def fpe_hash_udf(val):
    if not val: return None
    # Simulated Format Preserving Encryption using salted hash for MRN/SSN
    h = hashlib.sha256(f"{val}{salt}".encode('utf-8')).hexdigest()
    return f"DEID-{h[:10]}"

def date_shift_udf(val, pid):
    if not val or not pid: return None
    # Deterministic date-shifting based on patient ID modulo using a stable hash
    # Shift between -30 and +30 days
    stable_hash = int(hashlib.sha256(pid.encode('utf-8')).hexdigest(), 16)
    shift_days = (stable_hash % 61) - 30
    return val + timedelta(days=shift_days)

fpe_hash = udf(fpe_hash_udf, StringType())
date_shift = udf(date_shift_udf, DateType())

def main(args):
    spark = SparkSession.builder \
        .appName("EHDIP_DEID_Engine") \
        .getOrCreate()

    # Read from Bronze (assuming we extracted JSON payload to columns for processing)
    df = spark.read \
        .format("iceberg") \
        .load(f"{args.catalog}.{args.database}.{args.input_table}")

    # Safe Harbor 18: removing explicit names, exact dates > 89 years etc.
    # We will simulate applying FPE and Date Shifting

    # We assume 'df' has been flattened or we are applying this to specific columns
    # Example logic applied to specific columns if they existed
    if "patient_ssn" in df.columns:
        df = df.withColumn("patient_ssn_deid", fpe_hash(col("patient_ssn")))

    if "patient_mrn" in df.columns:
        df = df.withColumn("patient_mrn_deid", fpe_hash(col("patient_mrn")))

    if "admission_date" in df.columns and "patient_id" in df.columns:
        df = df.withColumn("admission_date_shifted", date_shift(col("admission_date"), col("patient_id")))

    # Free-text Safe Harbor redaction (Simulated regex)
    if "clinical_notes" in df.columns:
        # Simple regex simulation for phone numbers or specific patterns
        df = df.withColumn("clinical_notes_redacted", regexp_replace(col("clinical_notes"), r'\d{3}-\d{2}-\d{4}', '[REDACTED_SSN]'))

    # Drop the original PHI columns to prevent leakage
    df = df.drop("patient_ssn", "patient_mrn", "admission_date")

    # Save to intermediate or directly to Silver (depending on pipeline step)
    df.write \
        .format("iceberg") \
        .mode("append") \
        .save(f"{args.catalog}.{args.database}.{args.output_table}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="De-Identification Engine")
    parser.add_argument("--catalog", default="glue_catalog", help="Iceberg Catalog")
    parser.add_argument("--database", default="ehdip_bronze_db", help="Iceberg Database")
    parser.add_argument("--input-table", required=True, help="Input Table")
    parser.add_argument("--output-table", required=True, help="Output Table")

    args = parser.parse_args()
    main(args)
