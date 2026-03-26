"""
Accessibility and quality of care: Quality of Care, 2016

Self-administered questionnaire (SAQ):
 - Number/percent of adults by ability to schedule a routine appointment
 - By insurance coverage status

Input file: C:/MEPS/h192.ssp (2016 full-year consolidated)

Replaces: SAS/summary_tables_examples/care_quality_2016.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_ssp
from utils.survey_utils import survey_freq
from utils.format_mappings import INSURANCE_FORMAT

# Initialize Spark
spark = get_spark()

# Load FYC file
h192 = load_ssp(spark, "C:/MEPS/h192.ssp")

# Define variables
meps = (
    h192
    # Domain: adults who made an appointment
    .withColumn("domain", F.when(
        (F.col("ADRTCR42") == 1) & (F.col("AGELAST") >= 18), 1
    ).otherwise(0))
    .withColumn("SAQWT16F", F.when(
        (F.col("domain") == 0) & (F.col("SAQWT16F") == 0), 1
    ).otherwise(F.col("SAQWT16F")))
)

# Apply format labels
meps = (
    meps
    .withColumn("freq_label",
        F.when(F.col("ADRTWW42") == 4, "Always")
        .when(F.col("ADRTWW42") == 3, "Usually")
        .when(F.col("ADRTWW42").isin(1, 2), "Sometimes/Never")
        .when(F.col("ADRTWW42").isin(-7, -8, -9), "Don't know/Non-response")
        .when(F.col("ADRTWW42") == -1, "Inapplicable")
        .otherwise("Missing"))
    .withColumn("insurance_label",
        F.when(F.col("INSURC16") == 1, "<65, Any private")
        .when(F.col("INSURC16") == 2, "<65, Public only")
        .when(F.col("INSURC16") == 3, "<65, Uninsured")
        .when(F.col("INSURC16") == 4, "65+, Medicare only")
        .when(F.col("INSURC16") == 5, "65+, Medicare and private")
        .when(F.col("INSURC16") == 6, "65+, Medicare and other public")
        .when(F.col("INSURC16").isin(7, 8), "65+, No medicare")
        .otherwise("Missing"))
)

# Calculate estimates using survey procedures
# Use SAQWT16F weight variable, since outcome variable comes from SAQ
print("=== Survey-weighted frequencies: Appointment scheduling by insurance ===")
domain_df = meps.filter(F.col("domain") == 1)

results = survey_freq(
    domain_df,
    table_vars=["freq_label", "insurance_label"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="SAQWT16F",
)
print(results.to_string())

# Ability to schedule a routine appointment (adults), by insurance coverage
print("\n=== Crosstab: Appointment scheduling by insurance ===")
(domain_df
    .filter(F.col("freq_label") != "Missing")
    .filter(F.col("insurance_label") != "Missing")
    .groupBy("freq_label", "insurance_label")
    .count()
    .orderBy("freq_label", "insurance_label")
    .show(50))

spark.stop()
