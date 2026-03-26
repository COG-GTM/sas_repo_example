"""
L1: Linking Jobs and Full-Year Files (2001 Data)

This example shows how to:
  (1) Identify jobs in the first part of 2001
  (2) Count the numbers of each type of job for each person
  (3) Merge JOBS and FY files
  (4) Calculate standard errors and relative standard errors

Migrated from: SAS/older_exercises_1996_to_2006/Linking_examples/L1/L1.sas
Input files:
  h56.sas7bdat (2001 Jobs File)
  h60.sas7bdat (2001 Full-Year Persons File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_freq

spark = get_spark()

# ---------------------------------------------------------------------------
# Load data files
# ---------------------------------------------------------------------------
h56 = load_sas7bdat(spark, "C:/MEPS/h56.sas7bdat")  # 2001 Jobs
h60 = load_sas7bdat(spark, "C:/MEPS/h60.sas7bdat")  # 2001 FY Persons

# ---------------------------------------------------------------------------
# Subset JOBS file to first-round records
# Panel 5/Round 3 or Panel 6/Round 1
# ---------------------------------------------------------------------------
jobs_rn1 = h56.filter(
    ((F.col("PANEL") == 5) & (F.col("RN") == 3)) |
    ((F.col("PANEL") == 6) & (F.col("RN") == 1))
)

print("=== All Jobs at Beginning of Year ===")
jobs_rn1.groupBy("SUBTYPE").count().orderBy("SUBTYPE").show()

# ---------------------------------------------------------------------------
# Get person-level counts of each type of job from first-round records
# ---------------------------------------------------------------------------
jobsper = (
    jobs_rn1
    .groupBy("DUPERSID")
    .agg(
        F.sum(F.when(F.col("SUBTYPE") == 1, 1).otherwise(0)).alias("N1"),  # Current Main
        F.sum(F.when(F.col("SUBTYPE") == 2, 1).otherwise(0)).alias("N2"),  # Current Misc
        F.sum(F.when(F.col("SUBTYPE") == 3, 1).otherwise(0)).alias("N3"),  # Former Main
        F.sum(F.when(F.col("SUBTYPE") == 4, 1).otherwise(0)).alias("N4"),  # Former Misc
        F.sum(F.when(F.col("SUBTYPE") == 5, 1).otherwise(0)).alias("N5"),  # Last Job Outside Rn
        F.sum(F.when(F.col("SUBTYPE") == 6, 1).otherwise(0)).alias("N6"),  # Retirement
        F.count("*").alias("TOTJOBS")
    )
)

print("=== Persons with First-Round Jobs Record ===")
jobsper.groupBy("TOTJOBS").count().orderBy("TOTJOBS").show()

# ---------------------------------------------------------------------------
# Combine person-level jobs file with full-year PUF
# ---------------------------------------------------------------------------
fy_persons = h60.select("DUPERSID", "PANEL01", "AGE31X", "PERWT01F",
                         "VARSTR01", "VARPSU01")

allper = fy_persons.join(jobsper, on="DUPERSID", how="left")

# Fill nulls for persons without jobs records
for col_name in ["N1", "N2", "N3", "N4", "N5", "N6", "TOTJOBS"]:
    allper = allper.fillna({col_name: 0})

# ---------------------------------------------------------------------------
# For persons age 18+: count of current main & miscellaneous jobs
# ---------------------------------------------------------------------------
curr = (
    allper
    .withColumn("NUMCURR", F.col("N1") + F.col("N2"))
    .withColumn("NCURR",
        F.when(F.col("NUMCURR") + 1 > 3, 4)
         .otherwise(F.col("NUMCURR") + 1)
    )
    .withColumn("NCURR_label",
        F.when(F.col("NCURR") == 1, "None")
         .when(F.col("NCURR") == 2, "1")
         .when(F.col("NCURR") == 3, "2")
         .when(F.col("NCURR") == 4, "3+")
         .otherwise("Unknown")
    )
    # Subpopulation: age 18+
    .withColumn("ADULT", F.when(F.col("AGE31X") >= 18, 1).otherwise(0))
)

# ---------------------------------------------------------------------------
# Survey estimates for adults age 18+
# ---------------------------------------------------------------------------
print("=== Persons Age 18+: Current Main & Misc Jobs at Beginning of Year ===")
adults = curr.filter(F.col("ADULT") == 1)

results = survey_freq(
    adults,
    table_vars=["NCURR_label"],
    stratum_col="VARSTR01",
    cluster_col="VARPSU01",
    weight_col="PERWT01F"
)
print(results)

spark.stop()
