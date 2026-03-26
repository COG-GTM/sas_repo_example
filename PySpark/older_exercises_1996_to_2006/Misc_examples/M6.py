"""
M6: Diabetes Care Supplement Weight Usage (2003 Data)

This example demonstrates how to use the Diabetes Care Supplement (DCS)
weight (DIABW03F) from the Self-Administered Questionnaire (SAQ) data.
The DCS weight should be used for analyses of the diabetes-specific
questions that are only asked of persons with diabetes who completed
the SAQ.

Migrated from: SAS/older_exercises_1996_to_2006/Misc_examples/M6/M6.sas
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

# ---------------------------------------------------------------------------
# Identify persons with diabetes who completed the SAQ
# DIABDX = 1 (ever diagnosed with diabetes)
# DIABW03F > 0 (positive DCS weight)
# ---------------------------------------------------------------------------
diab_saq = (
    h79
    .filter(
        (F.col("DIABDX") == 1) &
        (F.col("DIABW03F") > 0)
    )
)

print("=== Persons with Diabetes who Completed SAQ ===")
print(f"  N (unweighted): {diab_saq.count()}")

# ---------------------------------------------------------------------------
# Analysis: Diabetes care measures using DCS weight
# DSA1C53: Had hemoglobin A1c test in past year (1=Yes, 2=No)
# DSFT53: Had feet checked in past year (1=Yes, 2=No)
# DSEY53: Had dilated eye exam in past year (1=Yes, 2=No)
# ---------------------------------------------------------------------------
diab_measures = (
    diab_saq
    .withColumn("HAD_A1C",
        F.when(F.col("DSA1C53") == 1, 100).otherwise(0)
    )
    .withColumn("HAD_FOOT",
        F.when(F.col("DSFT53") == 1, 100).otherwise(0)
    )
    .withColumn("HAD_EYE",
        F.when(F.col("DSEY53") == 1, 100).otherwise(0)
    )
)

# ---------------------------------------------------------------------------
# Survey-weighted estimates using DCS weight
# ---------------------------------------------------------------------------
print("\n=== Diabetes Care Measures (Using DCS Weight DIABW03F) ===")
results = survey_mean(
    diab_measures,
    var_cols=["HAD_A1C", "HAD_FOOT", "HAD_EYE"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="DIABW03F"
)
print(results)

# ---------------------------------------------------------------------------
# For comparison: Using person weight instead (INCORRECT for DCS items)
# ---------------------------------------------------------------------------
print("\n=== Same Measures Using Person Weight (INCORRECT for DCS) ===")
results_pw = survey_mean(
    diab_measures,
    var_cols=["HAD_A1C", "HAD_FOOT", "HAD_EYE"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F"
)
print(results_pw)

print("\nNote: The DCS weight (DIABW03F) should be used for analyses of")
print("diabetes-specific questions from the SAQ, not the person weight.")

spark.stop()
