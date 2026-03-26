"""
L1A: Linking 2000 and 2001 Jobs Files

This example shows how to:
  (1) Identify 2001 jobs that began in 2000
  (2) Identify first-reported 2000 jobs
  (3) Update missing 2001 values with 2000 values

Migrated from: SAS/older_exercises_1996_to_2006/Linking_examples/L1A/L1A.sas
Input files:
  h40.sas7bdat (2000 Jobs File)
  h56.sas7bdat (2001 Jobs File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark

spark = get_spark()

# ---------------------------------------------------------------------------
# Load data files
# ---------------------------------------------------------------------------
h40 = load_sas7bdat(spark, "C:/MEPS/h40.sas7bdat")  # 2000 Jobs
h56 = load_sas7bdat(spark, "C:/MEPS/h56.sas7bdat")  # 2001 Jobs

# ---------------------------------------------------------------------------
# Identify 2001 Current Main Jobs (Panel 5, Round 3) that began in 2000
# (STILLAT=1 means still at this job)
# ---------------------------------------------------------------------------
cmj01 = (
    h56
    .filter(
        (F.col("PANEL") == 5) &
        (F.col("RN") == 3) &
        (F.col("SUBTYPE") == 1) &
        (F.col("STILLAT") == 1)
    )
    .select("DUPERSID", "PANEL", "RN", "JOBSN", "SUBTYPE", "STILLAT",
            "SICKPAY", "PAYDRVST", "PAYVACTN")
)

print("=== 2001 (Panel 5 Round 3) Records ===")
for col_name in ["SICKPAY", "PAYDRVST", "PAYVACTN"]:
    cmj01.groupBy(col_name).count().orderBy(col_name).show()

# ---------------------------------------------------------------------------
# Identify newly-reported 2000 CMJs (Panel 5, Rounds 1/2)
# STILLAT=-1 means newly reported
# ---------------------------------------------------------------------------
cmj00 = (
    h40
    .filter(
        (F.col("PANEL") == 5) &
        (F.col("RN").isin(1, 2)) &
        (F.col("SUBTYPE") == 1) &
        (F.col("STILLAT") == -1)
    )
    .select("DUPERSID", "PANEL", "RN", "JOBSN", "SUBTYPE", "STILLAT",
            "SICKPAY", "PAYDRVST", "PAYVACTN")
)

print("=== 2000 (Panel 5 Round 1,2) Records ===")
for col_name in ["SICKPAY", "PAYDRVST", "PAYVACTN"]:
    cmj00.groupBy(col_name).count().orderBy(col_name).show()

# ---------------------------------------------------------------------------
# Merge records from both years to update missing 2001 values
# Rename 2000 variables before merge
# ---------------------------------------------------------------------------
cmj00_renamed = (
    cmj00
    .withColumnRenamed("SICKPAY", "SICKPAYX")
    .withColumnRenamed("PAYDRVST", "PAYDRVSTX")
    .withColumnRenamed("PAYVACTN", "PAYVACTNX")
    .drop("STILLAT", "RN")
    .select("DUPERSID", "JOBSN", "SICKPAYX", "PAYDRVSTX", "PAYVACTNX")
)

new_data = (
    cmj01
    .join(cmj00_renamed, on=["DUPERSID", "JOBSN"], how="inner")
)

print("=== Merged Records: Updated Variables ===")
new_data.select("DUPERSID", "JOBSN", "SUBTYPE", "STILLAT",
                "SICKPAYX", "PAYDRVSTX", "PAYVACTNX").show(60)

# ---------------------------------------------------------------------------
# Cross-tabulation: original vs. first-reported values
# ---------------------------------------------------------------------------
print("=== SICKPAY (2001) vs SICKPAYX (first-reported) ===")
new_data.groupBy("SICKPAYX", "SICKPAY").count().orderBy(
    "SICKPAYX", "SICKPAY"
).show(50)

print("=== PAYDRVST (2001) vs PAYDRVSTX (first-reported) ===")
new_data.groupBy("PAYDRVSTX", "PAYDRVST").count().orderBy(
    "PAYDRVSTX", "PAYDRVST"
).show(50)

print("=== PAYVACTN (2001) vs PAYVACTNX (first-reported) ===")
new_data.groupBy("PAYVACTNX", "PAYVACTN").count().orderBy(
    "PAYVACTNX", "PAYVACTN"
).show(50)

spark.stop()
