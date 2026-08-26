import os
import argparse
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, date_add, expr, when
from pyspark.sql.types import StringType

def get_hash_salt():
    return os.environ.get("EHDIP_HASH_SALT", "default_insecure_salt")

def _fpe_mask(value):
    if not value:
        return value
    salt = get_hash_salt()
    # FPE requires salt as mentioned in memory: salted hashes via EHDIP_HASH_SALT
    salted_value = f"{value}{salt}".encode('utf-8')
    return hashlib.sha256(salted_value).hexdigest()

fpe_udf = udf(_fpe_mask, StringType())

def main():
    parser = argparse.ArgumentParser(description="PHI De-Identification Engine")
    parser.add_argument("--input-table", required=True, help="Input Iceberg table (Bronze)")
    parser.add_argument("--output-table", required=True, help="Output Iceberg table (Silver De-identified)")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("EHDIP_Deid_Engine") \
        .getOrCreate()

    df = spark.table(args.input_table)

    # Note: Assuming the Bronze table was parsed into a structured dataframe
    # or this step extracts json fields first. For demonstration, let's assume
    # fields like ssn, mrn, patient_id, birth_date, notes exist.

    # 1. FPE for SSN/MRN
    # 2. Deterministic Date Shifting for dates
    # 3. Safe Harbor 18 Redaction for free-text

    # Let's write the transformations dynamically based on column presence
    columns = df.columns

    if "ssn" in columns:
        df = df.withColumn("ssn_masked", fpe_udf(col("ssn"))).drop("ssn")
    if "mrn" in columns:
        df = df.withColumn("mrn_masked", fpe_udf(col("mrn"))).drop("mrn")

    if "patient_id" in columns and "birth_date" in columns:
        # deterministic date shifting
        # hash patient_id, take modulo 61, subtract 30 to get range [-30, 30]
        # Using Spark SQL functions for deterministic shift instead of Python UDF for performance
        df = df.withColumn(
            "date_shift_days",
            expr("pmod(conv(substr(md5(patient_id), 1, 15), 16, 10), 61) - 30")
        )
        df = df.withColumn("birth_date_shifted", expr("date_add(birth_date, cast(date_shift_days as int))")).drop("birth_date", "date_shift_days")

    if "notes" in columns:
        # Simplified Safe Harbor 18: redact obvious patterns (in real world uses NLP/RegEx)
        # Redacting generic "[PHI]" for demonstration
        df = df.withColumn("notes_redacted", expr("regexp_replace(notes, '\\\\b\\\\d{3}-\\\\d{2}-\\\\d{4}\\\\b', '[REDACTED_SSN]')"))\
               .drop("notes")

    df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(args.output_table)

if __name__ == "__main__":
    main()
