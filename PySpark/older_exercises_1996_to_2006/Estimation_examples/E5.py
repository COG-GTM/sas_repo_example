"""
E5: Event-Level Estimates (2001 Data)

This example shows how to compute event-level estimates for inpatient
stays and office-based visits directly from event files (no aggregation
needed since event files are already at the event level).

Migrated from: SAS/older_exercises_1996_to_2006/Estimation_examples/E5/E5.sas
Input files:
  h59d.sas7bdat (2001 Inpatient Stays)
  h59g.sas7bdat (2001 Office-Based Visits)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# ---------------------------------------------------------------------------
# Load 2001 event files
# ---------------------------------------------------------------------------
h59d = load_sas7bdat(spark, "C:/MEPS/h59d.sas7bdat")  # 2001 Inpatient Stays
h59g = load_sas7bdat(spark, "C:/MEPS/h59g.sas7bdat")  # 2001 Office-Based Visits

# ---------------------------------------------------------------------------
# Inpatient Stay Estimates
# ---------------------------------------------------------------------------
print("=== Inpatient Stay Expenditures (Event Level) ===")
ip_results = survey_mean(
    h59d,
    var_cols=["IPXP01X"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT01F"
)
print(ip_results)

print("\n=== Total Inpatient Stay Expenditures ===")
ip_total = survey_total(
    h59d,
    var_cols=["IPXP01X"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT01F"
)
print(ip_total)

# ---------------------------------------------------------------------------
# Office-Based Visit Estimates
# ---------------------------------------------------------------------------
print("\n=== Office-Based Visit Expenditures (Event Level) ===")
ob_results = survey_mean(
    h59g,
    var_cols=["OBXP01X"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT01F"
)
print(ob_results)

print("\n=== Total Office-Based Visit Expenditures ===")
ob_total = survey_total(
    h59g,
    var_cols=["OBXP01X"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT01F"
)
print(ob_total)

spark.stop()
