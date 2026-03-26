"""
E8: Healthcare Expenditure Distribution and Inpatient Stay Analysis (2005 Data)

This example generates expenditure data comparable to MEPS Statistical Brief #164,
including distribution by event type, distribution of inpatient expenses by source
of payment, and per-stay/per-diem inpatient expenses with and without surgery.

Expected output:
  Total Health Care Exp: $963,882,049,581 (SB: $963.9 billion)
  PMED: 20.8%, Hospital: 30.9%, Ambulatory: 34.7%, Other: 13.5%

Migrated from: SAS/older_exercises_1996_to_2006/Estimation_examples/E8/E8.sas
Input files:
  h97.sas7bdat (2005 Full-Year Consolidated Data File)
  h94d.sas7bdat (2005 Hospital Inpatient Stays)
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
h97 = load_sas7bdat(spark, "C:/MEPS/h97.sas7bdat")   # 2005 FYC
h94d = load_sas7bdat(spark, "C:/MEPS/h94d.sas7bdat")  # 2005 IP Stays

# ---------------------------------------------------------------------------
# Create expenditure variables by event type and source of payment
# ---------------------------------------------------------------------------
puf97 = (
    h97
    .withColumn("TOTAL", F.col("TOTEXP05"))
    .withColumn("PRESCRIBED_MEDICINES", F.col("RXEXP05"))
    .withColumn("HOSPITAL_INPATIENT", F.col("IPDEXP05") + F.col("IPFEXP05"))
    .withColumn("AMBULATORY_CARE",
        F.col("OBVEXP05") + F.col("OPDEXP05") + F.col("OPFEXP05") +
        F.col("ERDEXP05") + F.col("ERFEXP05")
    )
    .withColumn("OTHER",
        F.col("TOTEXP05") - (
            F.col("RXEXP05") +
            F.col("IPDEXP05") + F.col("IPFEXP05") +
            F.col("OBVEXP05") + F.col("OPDEXP05") + F.col("OPFEXP05") +
            F.col("ERDEXP05") + F.col("ERFEXP05")
        )
    )
    # IP source of payment
    .withColumn("IP_TOTAL", F.col("IPDEXP05") + F.col("IPFEXP05"))
    .withColumn("IP_PRIVATE_INS",
        F.col("IPDPRV05") + F.col("IPFPRV05") +
        F.col("IPDTRI05") + F.col("IPFTRI05")
    )
    .withColumn("IP_MEDICARE", F.col("IPDMCR05") + F.col("IPFMCR05"))
    .withColumn("IP_MEDICAID", F.col("IPDMCD05") + F.col("IPFMCD05"))
    .withColumn("IP_OUT_OF_POCKET", F.col("IPDSLF05") + F.col("IPFSLF05"))
    .withColumn("IP_OTHER",
        (F.col("IPDEXP05") + F.col("IPFEXP05")) - (
            F.col("IPDPRV05") + F.col("IPFPRV05") +
            F.col("IPDTRI05") + F.col("IPFTRI05") +
            F.col("IPDMCR05") + F.col("IPFMCR05") +
            F.col("IPDMCD05") + F.col("IPFMCD05") +
            F.col("IPDSLF05") + F.col("IPFSLF05")
        )
    )
)

# ---------------------------------------------------------------------------
# Figure 1: Distribution of expenditures by event type
# ---------------------------------------------------------------------------
print("=== Figure 1: Total Expenditures by Event Type ===")
results_type = survey_total(
    puf97,
    var_cols=["PRESCRIBED_MEDICINES", "HOSPITAL_INPATIENT",
              "AMBULATORY_CARE", "OTHER", "TOTAL"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT05F"
)
print(results_type)

# ---------------------------------------------------------------------------
# Figure 1: Distribution of IP expenses by source of payment
# ---------------------------------------------------------------------------
print("\n=== IP Expenditures by Source of Payment ===")
results_sop = survey_total(
    puf97,
    var_cols=["IP_PRIVATE_INS", "IP_MEDICARE", "IP_MEDICAID",
              "IP_OUT_OF_POCKET", "IP_OTHER", "IP_TOTAL"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT05F"
)
print(results_sop)

# ---------------------------------------------------------------------------
# Figure 5: IP expenses per stay and per diem, with/without surgery
# ---------------------------------------------------------------------------
# Prepare IP stays data
ip2005 = (
    h94d
    # If zero nights, set to 1 for per diem calculation
    .withColumn("NUMNIGHX",
        F.when(F.col("NUMNIGHX") == 0, 1).otherwise(F.col("NUMNIGHX"))
    )
    .withColumn("PERDIEM", F.round(F.col("IPXP05X") / F.col("NUMNIGHX")))
    # Surgery label (RSNINHOS=1 is surgery)
    .withColumn("RSNINHOS_label",
        F.when(F.col("RSNINHOS") == 1, "Surgery")
         .otherwise("No Surgery")
    )
)

print("\n=== Figure 5: Average IP Expenses Per Stay by Surgery Status ===")
results_stay = survey_mean(
    ip2005,
    var_cols=["IPXP05X"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT05F",
    domain_col="RSNINHOS_label"
)
print(results_stay)

print("\n=== Figure 5: Average IP Expenses Per Diem by Surgery Status ===")
results_diem = survey_mean(
    ip2005,
    var_cols=["PERDIEM"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT05F",
    domain_col="RSNINHOS_label"
)
print(results_diem)

spark.stop()
