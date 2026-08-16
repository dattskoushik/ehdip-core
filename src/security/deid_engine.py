from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, date_add
from pyspark.sql.types import StringType
import hashlib
import re

def deterministic_fpe_mock(val: str) -> str:
    """Mock Format-Preserving Encryption for demo purposes."""
    if not val:
        return val
    # Simple hash based masking preserving length where possible for demo
    hashed = hashlib.sha256(val.encode()).hexdigest()
    return hashed[:len(val)] if len(val) <= 64 else hashed

fpe_udf = udf(deterministic_fpe_mock, StringType())

def redact_free_text_mock(text: str) -> str:
    """Mock Safe Harbor 18 redaction (regex based)."""
    if not text:
        return text
    # Very basic regex to redact things that look like SSNs or Phone numbers
    text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[REDACTED_SSN]', text)
    text = re.sub(r'\b\d{3}-\d{3}-\d{4}\b', '[REDACTED_PHONE]', text)
    return text

redact_udf = udf(redact_free_text_mock, StringType())

def apply_deidentification(df):
    """
    Applies FPE for SSN/MRN, deterministic date-shifting, and free-text redaction.
    Assumes incoming df has structured columns: ssn, mrn, birth_date, notes
    """
    deid_df = df \
        .withColumn("ssn_fpe", fpe_udf(col("ssn"))) \
        .withColumn("mrn_fpe", fpe_udf(col("mrn"))) \
        .withColumn("birth_date_shifted", date_add(col("birth_date"), -30)) \
        .withColumn("notes_redacted", redact_udf(col("notes"))) \
        .drop("ssn", "mrn", "birth_date", "notes")

    return deid_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DeId_Engine").getOrCreate()
    # df = spark.read.parquet("s3://ehdip-bronze-data-lake/structured/")
    # deid_df = apply_deidentification(df)
    # deid_df.write.parquet("s3://ehdip-bronze-data-lake/deidentified/")
