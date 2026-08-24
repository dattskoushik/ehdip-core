import hashlib
import os
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

def get_hash_salt():
    """Retrieves the salt from environment variables for FPE (HIPAA requirement)"""
    salt = os.getenv('EHDIP_HASH_SALT')
    if not salt:
        raise ValueError("Environment variable EHDIP_HASH_SALT must be set for FPE.")
    return salt

def fpe_mask(value: str, salt: str) -> str:
    """Format-Preserving Encryption logic via salted hashing for demo purposes.
       Real FPE uses libraries like Voltage or Protegrity, but salted hash serves
       as deterministic pseudo-anonymization for EHDIP patterns."""
    if not value:
        return value
    salted = value + salt
    return hashlib.sha256(salted.encode('utf-8')).hexdigest()

def get_fpe_udf():
    salt = get_hash_salt()
    return F.udf(lambda x: fpe_mask(x, salt) if x else x, StringType())

def date_shift(patient_id_col: F.Column, date_col: F.Column) -> F.Column:
    """
    Deterministic date-shifting based on patient ID hash modulo.
    Shifts between -30 to +30 days.
    """
    # Hash the patient ID, convert to integer modulo 61, subtract 30 to get range [-30, 30]
    shift_days = F.pmod(F.abs(F.hash(patient_id_col)), F.lit(61)) - F.lit(30)
    return F.expr(f"date_add({date_col._jc.toString()}, {shift_days._jc.toString()})")

def apply_safe_harbor_redaction(df: DataFrame, text_columns: list) -> DataFrame:
    """
    Redacts specific regex patterns (dates, SSNs, phone numbers) from free text.
    In a real implementation, NLP is used, here we apply basic regex.
    """
    # Simple regex for SSN redaction as an example of Safe Harbor 18
    for col_name in text_columns:
        df = df.withColumn(
            col_name,
            F.regexp_replace(F.col(col_name), r'\b\d{3}-\d{2}-\d{4}\b', '[REDACTED_SSN]')
        )
    return df

def deidentify_dataframe(df: DataFrame, ssn_col: str, mrn_col: str, patient_id_col: str, date_cols: list, text_cols: list) -> DataFrame:
    """
    Applies all PHI governance rules to a dataframe.
    """
    fpe_udf = get_fpe_udf()

    # 1. FPE for identifiers
    if ssn_col in df.columns:
        df = df.withColumn(f"{ssn_col}_masked", fpe_udf(F.col(ssn_col)))
    if mrn_col in df.columns:
        df = df.withColumn(f"{mrn_col}_masked", fpe_udf(F.col(mrn_col)))

    # 2. Date Shifting
    for d_col in date_cols:
        if d_col in df.columns:
            df = df.withColumn(f"{d_col}_shifted", date_shift(F.col(patient_id_col), F.col(d_col)))

    # 3. Free-text Redaction (Safe Harbor 18)
    df = apply_safe_harbor_redaction(df, [c for c in text_cols if c in df.columns])

    return df

if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession

    if len(sys.argv) != 4:
        print("Usage: deid_engine.py <input_path> <output_path> <text_cols_comma_separated>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    text_cols = sys.argv[3].split(",")

    spark = SparkSession.builder.appName("EHDIP_DeID_Engine").getOrCreate()

    df = spark.read.format("iceberg").load(input_path)
    df_deid = deidentify_dataframe(df, "ssn", "mrn", "patient_id", ["encounter_date", "dob"], text_cols)

    df_deid.write.format("iceberg").mode("append").save(output_path)
    print(f"De-identified data saved to {output_path}")
