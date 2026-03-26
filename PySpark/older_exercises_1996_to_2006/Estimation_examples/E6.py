"""
E6: Percentage Distribution by Type of Service (2005 Data)

This example shows how to compute the percentage distribution of total
health care expenditures by type of service, and the percentage of
persons with an expense by type of service.

Migrated from: SAS/older_exercises_1996_to_2006/Estimation_examples/E6/E6.sas
Input file: h97.sas7bdat (2005 Full-Year Consolidated Data File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# ---------------------------------------------------------------------------
# Load 2005 Full-Year Consolidated Data File (HC-097)
# ---------------------------------------------------------------------------
h97 = load_sas7bdat(spark, "C:/MEPS/h97.sas7bdat")

# ---------------------------------------------------------------------------
# Create expenditure variables by type of service
# ---------------------------------------------------------------------------
meps = (
    h97
    # Hospital Inpatient (facility + doctor)
    .withColumn("HOSPITAL_INPATIENT", F.col("IPDEXP05") + F.col("IPFEXP05"))
    # Ambulatory (office-based + outpatient + ER, facility + doctor)
    .withColumn("AMBULATORY",
        F.col("OBVEXP05") + F.col("OPDEXP05") + F.col("OPFEXP05") +
        F.col("ERDEXP05") + F.col("ERFEXP05")
    )
    # Prescribed Medicines
    .withColumn("PMED", F.col("RXEXP05"))
    # Home Health
    .withColumn("HOME_HEALTH", F.col("HHAEXP05") + F.col("HHNEXP05"))
    # Total
    .withColumn("TOTAL", F.col("TOTEXP05"))
    # Other = Total - (Hospital + Ambulatory + PMED)
    .withColumn("OTHER",
        F.col("TOTEXP05") - (
            F.col("HOSPITAL_INPATIENT") + F.col("AMBULATORY") +
            F.col("PMED")
        )
    )
    # Flag variables: 1 if expense > 0, else 0
    .withColumn("HAS_IP", F.when(F.col("HOSPITAL_INPATIENT") > 0, 1).otherwise(0))
    .withColumn("HAS_AMB", F.when(F.col("AMBULATORY") > 0, 1).otherwise(0))
    .withColumn("HAS_PMED", F.when(F.col("PMED") > 0, 1).otherwise(0))
    .withColumn("HAS_HH", F.when(F.col("HOME_HEALTH") > 0, 1).otherwise(0))
    .withColumn("HAS_OTHER", F.when(F.col("OTHER") > 0, 1).otherwise(0))
    .withColumn("HAS_ANY", F.when(F.col("TOTAL") > 0, 1).otherwise(0))
)

# ---------------------------------------------------------------------------
# Total expenditures by type of service (for percentage distribution)
# ---------------------------------------------------------------------------
print("=== Total Expenditures by Type of Service ===")
results_total = survey_total(
    meps,
    var_cols=["HOSPITAL_INPATIENT", "AMBULATORY", "PMED", "OTHER", "TOTAL"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT05F"
)
print(results_total)

# ---------------------------------------------------------------------------
# Mean expenditures per person by type of service
# ---------------------------------------------------------------------------
print("\n=== Mean Expenditures per Person by Type of Service ===")
results_mean = survey_mean(
    meps,
    var_cols=["HOSPITAL_INPATIENT", "AMBULATORY", "PMED", "OTHER", "TOTAL"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT05F"
)
print(results_mean)

# ---------------------------------------------------------------------------
# Percentage of persons with expense by type of service
# ---------------------------------------------------------------------------
print("\n=== Percentage of Persons with Expense by Type of Service ===")
results_pct = survey_mean(
    meps,
    var_cols=["HAS_IP", "HAS_AMB", "HAS_PMED", "HAS_HH", "HAS_OTHER", "HAS_ANY"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT05F"
)
print(results_pct)

spark.stop()
