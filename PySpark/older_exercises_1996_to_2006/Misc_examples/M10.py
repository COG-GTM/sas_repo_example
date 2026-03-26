"""
M10: Hospital Inpatient Expenditure Comparison (2003 Data)

This example compares estimates of hospital inpatient expenditures
from two sources:
  (1) Full-Year Consolidated (FYC) file (person-level)
  (2) Inpatient Stays (IP) event file (event-level)

Migrated from: SAS/older_exercises_1996_to_2006/Misc_examples/M10/M10.sas
Input files:
  h79.sas7bdat (2003 Full-Year Consolidated Data File)
  h77d.sas7bdat (2003 Hospital Inpatient Stays)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# ---------------------------------------------------------------------------
# Load data files
# ---------------------------------------------------------------------------
h79 = load_sas7bdat(spark, "C:/MEPS/h79.sas7bdat")   # 2003 FYC
h77d = load_sas7bdat(spark, "C:/MEPS/h77d.sas7bdat")  # 2003 IP Stays

# ---------------------------------------------------------------------------
# Method 1: From FYC file (person-level IP expenditures)
# ---------------------------------------------------------------------------
fyc = (
    h79
    .filter(F.col("PERWT03F") > 0)
    # Hospital inpatient expenditures = facility + doctor
    .withColumn("IP_EXP", F.col("IPDEXP03") + F.col("IPFEXP03"))
    # Flag: had any IP expense
    .withColumn("ANY_IP",
        F.when(F.col("IP_EXP") > 0, 1).otherwise(0)
    )
)

print("=== Method 1: FYC File - Person-Level IP Expenditures ===")

# Total IP expenditures
print("Total IP Expenditures:")
results_fyc_total = survey_total(
    fyc,
    var_cols=["IP_EXP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F"
)
print(results_fyc_total)

# Mean IP expenditures (all persons)
print("\nMean IP Expenditures (All Persons):")
results_fyc_mean_all = survey_mean(
    fyc,
    var_cols=["IP_EXP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F"
)
print(results_fyc_mean_all)

# Mean IP expenditures (persons with IP expense only)
print("\nMean IP Expenditures (Persons with IP Expense):")
fyc_users = fyc.filter(F.col("ANY_IP") == 1)
results_fyc_mean_users = survey_mean(
    fyc_users,
    var_cols=["IP_EXP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F"
)
print(results_fyc_mean_users)

# ---------------------------------------------------------------------------
# Method 2: From IP event file (event-level)
# ---------------------------------------------------------------------------
print("\n=== Method 2: IP Event File - Event-Level Expenditures ===")

# Aggregate IP events to person level
ip_person = (
    h77d
    .groupBy("DUPERSID")
    .agg(
        F.sum("IPXP03X").alias("IP_EXP_EVENTS"),
        F.count("*").alias("N_IP_STAYS")
    )
)

# Merge with FYC for weights
ip_merged = (
    h79
    .filter(F.col("PERWT03F") > 0)
    .select("DUPERSID", "PERWT03F", "VARSTR", "VARPSU")
    .join(ip_person, on="DUPERSID", how="left")
    .fillna({"IP_EXP_EVENTS": 0, "N_IP_STAYS": 0})
)

# Total IP expenditures from event file
print("Total IP Expenditures (from event file):")
results_ip_total = survey_total(
    ip_merged,
    var_cols=["IP_EXP_EVENTS"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F"
)
print(results_ip_total)

# Mean IP expenditures from event file (all persons)
print("\nMean IP Expenditures (All Persons, from event file):")
results_ip_mean = survey_mean(
    ip_merged,
    var_cols=["IP_EXP_EVENTS"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F"
)
print(results_ip_mean)

# Per-stay expenditures
print("\n=== Per-Stay IP Expenditures ===")
results_per_stay = survey_mean(
    h77d,
    var_cols=["IPXP03X"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F"
)
print(results_per_stay)

spark.stop()
