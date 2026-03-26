"""
M2: Simple Random Sample vs Complex Design Variance (2003 Data)

This example demonstrates how estimates and standard errors differ
depending on whether the complex survey design is accounted for.
Three approaches are compared:
  (1) Unweighted estimates
  (2) Weighted estimates ignoring complex design
  (3) Weighted estimates with complex design (STRATA + CLUSTER)

Migrated from: SAS/older_exercises_1996_to_2006/Misc_examples/M2/M2.sas
Input file: h79.sas7bdat (2003 Full-Year Consolidated Data File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_mean

spark = get_spark()

# ---------------------------------------------------------------------------
# Load 2003 Full-Year Consolidated Data File (HC-079)
# ---------------------------------------------------------------------------
h79 = load_sas7bdat(spark, "C:/MEPS/h79.sas7bdat")

# Filter to persons with positive weight
meps = h79.filter(F.col("PERWT03F") > 0)

# ---------------------------------------------------------------------------
# (1) Unweighted mean total expenditures
# ---------------------------------------------------------------------------
print("=== (1) Unweighted Mean Total Expenditures ===")
unweighted = meps.agg(
    F.mean("TOTEXP03").alias("Mean"),
    F.stddev("TOTEXP03").alias("StdDev"),
    F.count("*").alias("N")
).collect()[0]
print(f"  Mean: {unweighted['Mean']:.2f}")
print(f"  StdDev: {unweighted['StdDev']:.2f}")
print(f"  N: {unweighted['N']}")
# Approximate SE = StdDev / sqrt(N)
import math
se_unweighted = unweighted['StdDev'] / math.sqrt(unweighted['N'])
print(f"  Approx SE (SRS): {se_unweighted:.2f}")

# ---------------------------------------------------------------------------
# (2) Weighted mean (ignoring complex design)
# Simple weighted mean without accounting for strata/clusters
# ---------------------------------------------------------------------------
print("\n=== (2) Weighted Mean (Ignoring Complex Design) ===")
pdf = meps.select("TOTEXP03", "PERWT03F").toPandas()
import numpy as np
wt_mean = np.average(pdf["TOTEXP03"], weights=pdf["PERWT03F"])
print(f"  Weighted Mean: {wt_mean:.2f}")
# SRS variance with weights
wt_sum = pdf["PERWT03F"].sum()
wt_var = np.average((pdf["TOTEXP03"] - wt_mean)**2, weights=pdf["PERWT03F"])
wt_se_srs = math.sqrt(wt_var / len(pdf))
print(f"  Approx SE (SRS with weights): {wt_se_srs:.2f}")

# ---------------------------------------------------------------------------
# (3) Weighted mean with complex design (proper SEs)
# Uses Taylor series linearization with strata and clusters
# ---------------------------------------------------------------------------
print("\n=== (3) Weighted Mean with Complex Survey Design ===")
results = survey_mean(
    meps,
    var_cols=["TOTEXP03"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F"
)
print(results)

print("\nNote: The complex design estimate accounts for stratification")
print("and clustering, providing correct standard errors.")

spark.stop()
