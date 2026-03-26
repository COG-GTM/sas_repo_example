"""
M7: Person-Level Prescribed Medicine Expenditures (2003 Data)

This example demonstrates how to aggregate prescribed medicine (PMED)
event-level records to the person level and compute person-level
estimates of PMED expenditures and utilization.

Migrated from: SAS/older_exercises_1996_to_2006/Misc_examples/M7/M7.sas
Input files:
  h77a.sas7bdat (2003 Prescribed Medicines File)
  h79.sas7bdat (2003 Full-Year Consolidated Data File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_mean

spark = get_spark()

# ---------------------------------------------------------------------------
# Load data files
# ---------------------------------------------------------------------------
h77a = load_sas7bdat(spark, "C:/MEPS/h77a.sas7bdat")  # 2003 PMED
h79 = load_sas7bdat(spark, "C:/MEPS/h79.sas7bdat")    # 2003 FYC

# ---------------------------------------------------------------------------
# Aggregate PMED records to person level
# ---------------------------------------------------------------------------
pmed_person = (
    h77a
    .groupBy("DUPERSID")
    .agg(
        F.sum("RXXP03X").alias("TOT_PMED_EXP"),    # Total PMED expenditures
        F.sum("RXSF03X").alias("OOP_PMED_EXP"),     # Out-of-pocket PMED
        F.count("*").alias("N_PMED_FILLS"),          # Number of fills
        F.countDistinct("RXNDC").alias("N_UNIQUE_DRUGS")  # Unique drugs
    )
)

print("=== Person-Level PMED Summary ===")
pmed_person.describe("TOT_PMED_EXP", "OOP_PMED_EXP",
                      "N_PMED_FILLS", "N_UNIQUE_DRUGS").show()

# ---------------------------------------------------------------------------
# Merge with Full-Year file
# ---------------------------------------------------------------------------
merged = (
    h79
    .filter(F.col("PERWT03F") > 0)
    .join(pmed_person, on="DUPERSID", how="left")
    .fillna({
        "TOT_PMED_EXP": 0, "OOP_PMED_EXP": 0,
        "N_PMED_FILLS": 0, "N_UNIQUE_DRUGS": 0
    })
    # Flag: had any PMED expense
    .withColumn("ANY_PMED",
        F.when(F.col("TOT_PMED_EXP") > 0, 1).otherwise(0)
    )
    .withColumn("ANY_PMED_label",
        F.when(F.col("ANY_PMED") == 1, "1 Has PMED Expense")
         .otherwise("2 No PMED Expense")
    )
    # Age category
    .withColumn("AGE",
        F.when(F.col("AGE03X") >= 0, F.col("AGE03X"))
         .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
         .when(F.col("AGE31X") >= 0, F.col("AGE31X"))
         .otherwise(-1)
    )
    .withColumn("AGECAT",
        F.when((F.col("AGE") >= 0) & (F.col("AGE") <= 17), "0-17")
         .when((F.col("AGE") >= 18) & (F.col("AGE") <= 44), "18-44")
         .when((F.col("AGE") >= 45) & (F.col("AGE") <= 64), "45-64")
         .when(F.col("AGE") >= 65, "65+")
         .otherwise("Unknown")
    )
)

# ---------------------------------------------------------------------------
# Survey-weighted estimates: Mean PMED expenditures overall
# ---------------------------------------------------------------------------
print("=== Mean PMED Expenditures per Person (Overall) ===")
results_overall = survey_mean(
    merged,
    var_cols=["TOT_PMED_EXP", "OOP_PMED_EXP", "N_PMED_FILLS"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F"
)
print(results_overall)

# ---------------------------------------------------------------------------
# By age category
# ---------------------------------------------------------------------------
print("\n=== Mean PMED Expenditures by Age Category ===")
results_age = survey_mean(
    merged,
    var_cols=["TOT_PMED_EXP", "OOP_PMED_EXP", "N_PMED_FILLS"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F",
    domain_col="AGECAT"
)
print(results_age)

# ---------------------------------------------------------------------------
# Among persons with PMED expense: Mean expenditures
# ---------------------------------------------------------------------------
print("\n=== Mean PMED Expenditures Among Users ===")
pmed_users = merged.filter(F.col("ANY_PMED") == 1)
results_users = survey_mean(
    pmed_users,
    var_cols=["TOT_PMED_EXP", "OOP_PMED_EXP", "N_PMED_FILLS"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F"
)
print(results_users)

spark.stop()
