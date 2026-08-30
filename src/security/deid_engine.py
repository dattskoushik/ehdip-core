import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat, lit, expr, date_add, udf
from pyspark.sql.types import StringType

# FPE pseudo-function for demo purposes, using SHA256 + salt
def apply_fpe(df, column_name, salt):
    return df.withColumn(
        f"{column_name}_fpe",
        sha2(concat(col(column_name), lit(salt)), 256)
    )

def date_shift(df, date_col, id_col):
    # Deterministic shift based on ID hash modulo 61 - 30 (range -30 to +30 days)
    # Using Spark SQL expr for simple modulo on hash
    # Note: absolute value of hash % 61 to avoid negative mod issues in some SQL dialects
    shift_expr = f"cast(abs(hash({id_col})) % 61 as int) - 30"
    return df.withColumn(f"{date_col}_shifted", expr(f"date_add({date_col}, {shift_expr})"))

def deidentify_data(spark, input_table, output_table):
    salt = os.getenv("EHDIP_HASH_SALT", "default_salt")

    # Read from Bronze (assuming we extracted JSON fields, or we are doing it here)
    # For simplicity, assuming a structured DataFrame exists or we parse JSON
    # Here we just demo the transform on a structured DF

    df = spark.read.table(input_table)

    # Example transformation if table has SSN, MRN, birth_date, patient_id
    # If the input is raw JSON, we would parse it first. We will assume standard columns for demo.

    if "ssn" in df.columns:
        df = apply_fpe(df, "ssn", salt)

    if "mrn" in df.columns:
        df = apply_fpe(df, "mrn", salt)

    if "birth_date" in df.columns and "patient_id" in df.columns:
        df = date_shift(df, "birth_date", "patient_id")

    # Safe harbor free text (mock redaction)
    @udf(returnType=StringType())
    def redact_notes(text):
        if not text:
            return text
        return text.replace("John", "[REDACTED]").replace("Doe", "[REDACTED]")

    if "clinical_notes" in df.columns:
        df = df.withColumn("clinical_notes_redacted", redact_notes(col("clinical_notes")))

    # Write to a safe area or back to Bronze as deid
    df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(output_table)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", required=True)
    parser.add_argument("--output_table", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("PHI_DeID_Engine") \
        .getOrCreate()

    deidentify_data(spark, args.input_table, args.output_table)
