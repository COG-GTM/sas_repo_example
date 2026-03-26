"""
M5: Priority Conditions Comparison (2003 Data)

This example demonstrates how to identify persons with priority conditions
from the Full-Year Consolidated (FYC) file and from the Medical Conditions
file, and compare the two methods. The FYC file contains indicator variables
for selected priority conditions, while the Conditions file has ICD-9 codes.

Migrated from: SAS/older_exercises_1996_to_2006/Misc_examples/M5/M5.sas
Input files:
  h79.sas7bdat (2003 Full-Year Consolidated Data File)
  h78.sas7bdat (2003 Medical Conditions File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_freq

spark = get_spark()

# ---------------------------------------------------------------------------
# Load data files
# ---------------------------------------------------------------------------
h79 = load_sas7bdat(spark, "C:/MEPS/h79.sas7bdat")  # 2003 FYC
h78 = load_sas7bdat(spark, "C:/MEPS/h78.sas7bdat")  # 2003 Conditions

# ---------------------------------------------------------------------------
# Method 1: Identify diabetes from FYC priority condition variable
# DIABDX = 1 if person was ever diagnosed with diabetes
# ---------------------------------------------------------------------------
fyc_diab = (
    h79
    .filter(F.col("PERWT03F") > 0)
    .withColumn("FYC_DIABETES",
        F.when(F.col("DIABDX") == 1, 1).otherwise(0)
    )
)

print("=== Method 1: Diabetes from FYC Priority Condition Variable ===")
fyc_diab.groupBy("DIABDX").agg(
    F.count("*").alias("Unweighted_N"),
    F.sum("PERWT03F").alias("Weighted_N")
).orderBy("DIABDX").show()

# ---------------------------------------------------------------------------
# Method 2: Identify diabetes from Conditions file (ICD9 250.xx)
# ---------------------------------------------------------------------------
cond_diab = (
    h78
    .filter(F.col("ICD9CODX").startswith("250"))
    .select("DUPERSID")
    .dropDuplicates(["DUPERSID"])
    .withColumn("COND_DIABETES", F.lit(1))
)

print("=== Method 2: Diabetes from Conditions File (ICD9=250.xx) ===")
print(f"  Unique persons with ICD9=250: {cond_diab.count()}")

# ---------------------------------------------------------------------------
# Compare the two methods
# ---------------------------------------------------------------------------
compare = (
    fyc_diab
    .join(cond_diab, on="DUPERSID", how="left")
    .fillna({"COND_DIABETES": 0})
)

print("\n=== Comparison: FYC DIABDX vs Conditions File ICD9=250 ===")
compare.groupBy("FYC_DIABETES", "COND_DIABETES").agg(
    F.count("*").alias("Unweighted_N"),
    F.sum("PERWT03F").alias("Weighted_N")
).orderBy("FYC_DIABETES", "COND_DIABETES").show()

# ---------------------------------------------------------------------------
# Same comparison for high blood pressure (HBP)
# FYC: HIBPDX = 1
# Conditions: ICD9 = 401-405
# ---------------------------------------------------------------------------
fyc_hbp = (
    h79
    .filter(F.col("PERWT03F") > 0)
    .withColumn("FYC_HBP",
        F.when(F.col("HIBPDX") == 1, 1).otherwise(0)
    )
)

cond_hbp = (
    h78
    .filter(
        (F.col("ICD9CODX").startswith("401")) |
        (F.col("ICD9CODX").startswith("402")) |
        (F.col("ICD9CODX").startswith("403")) |
        (F.col("ICD9CODX").startswith("404")) |
        (F.col("ICD9CODX").startswith("405"))
    )
    .select("DUPERSID")
    .dropDuplicates(["DUPERSID"])
    .withColumn("COND_HBP", F.lit(1))
)

compare_hbp = (
    fyc_hbp
    .join(cond_hbp, on="DUPERSID", how="left")
    .fillna({"COND_HBP": 0})
)

print("\n=== Comparison: FYC HIBPDX vs Conditions File ICD9=401-405 ===")
compare_hbp.groupBy("FYC_HBP", "COND_HBP").agg(
    F.count("*").alias("Unweighted_N"),
    F.sum("PERWT03F").alias("Weighted_N")
).orderBy("FYC_HBP", "COND_HBP").show()

# ---------------------------------------------------------------------------
# Survey-weighted prevalence estimates
# ---------------------------------------------------------------------------
print("\n=== Survey-Weighted Diabetes Prevalence (FYC) ===")
results_diab = survey_freq(
    compare.withColumn("FYC_DIABETES_label",
        F.when(F.col("FYC_DIABETES") == 1, "1 Diabetes")
         .otherwise("2 No Diabetes")),
    table_vars=["FYC_DIABETES_label"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F"
)
print(results_diab)

spark.stop()
