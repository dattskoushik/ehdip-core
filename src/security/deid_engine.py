from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, date_add, lit
from pyspark.sql.types import StringType
import hashlib
import json

def get_spark_session():
    return SparkSession.builder \
        .appName("EHDIP_PHI_Deidentification") \
        .getOrCreate()

# Mock Format-Preserving Encryption (FPE) for SSN/MRN
# In a real environment, this would call out to a KMS or a dedicated FPE library (like Protegrity or similar)
def mock_fpe_encrypt(identifier: str) -> str:
    if not identifier: return None
    # A simple deterministic hash mimicking tokenization
    return "TKN-" + hashlib.sha256(identifier.encode('utf-8')).hexdigest()[:10]

fpe_udf = udf(mock_fpe_encrypt, StringType())

# Mock Free-text Safe Harbor Redaction
def redact_clinical_notes(text: str) -> str:
    if not text: return None
    # In production, use NLP (e.g. AWS Comprehend Medical or Spark NLP for Healthcare)
    # This is a naive regex-based or string replacement mock
    return text.replace("John Doe", "[NAME]").replace("123 Main St", "[ADDRESS]")

redact_udf = udf(redact_clinical_notes, StringType())

def apply_deidentification(df):
    """
    Applies de-identification rules:
    - FPE on MRN and SSN
    - Date shifting (+/- 30 days based on a deterministic salt)
    - Safe Harbor redaction on free-text notes
    """

    # Assuming the input dataframe is a parsed Silver-level table

    # 1. Apply FPE to identifiers
    if "mrn" in df.columns:
        df = df.withColumn("mrn_tokenized", fpe_udf(col("mrn"))).drop("mrn")
    if "ssn" in df.columns:
        df = df.withColumn("ssn_tokenized", fpe_udf(col("ssn"))).drop("ssn")

    # 2. Deterministic Date Shifting (Mock: shift all dates by a fixed amount for a patient)
    # In a real scenario, the shift amount would be deterministic based on the patient ID
    if "encounter_date" in df.columns:
         # Hardcoding a 15 day shift for demonstration
        df = df.withColumn("encounter_date_shifted", date_add(col("encounter_date"), 15)).drop("encounter_date")

    # 3. Text redaction
    if "clinical_notes" in df.columns:
        df = df.withColumn("clinical_notes_redacted", redact_udf(col("clinical_notes"))).drop("clinical_notes")

    return df

if __name__ == "__main__":
    spark = get_spark_session()
    # Logic to read from Bronze, parse JSON, apply deid, and write to Silver would go here
    print("De-id Engine loaded.")
