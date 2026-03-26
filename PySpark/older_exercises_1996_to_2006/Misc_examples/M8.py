"""
M8: Condition-PMED Linkage with De-Duplication (2003 Data)

This example demonstrates how to link conditions to prescribed medicines
using the CLNK file, handle de-duplication when a fill is linked to
multiple conditions, and compute person-level expenditures for specific
conditions.

Migrated from: SAS/older_exercises_1996_to_2006/Misc_examples/M8/M8.sas
Input files:
  h78.sas7bdat (2003 Conditions File)
  h78if1.sas7bdat (2003 CLNK File)
  h77a.sas7bdat (2003 Prescribed Medicines File)
  h79.sas7bdat (2003 Full-Year Consolidated Data File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_mean

spark = get_spark()

# ---------------------------------------------------------------------------
# Load data files
# ---------------------------------------------------------------------------
h78 = load_sas7bdat(spark, "C:/MEPS/h78.sas7bdat")     # 2003 Conditions
clnk = load_sas7bdat(spark, "C:/MEPS/h78if1.sas7bdat")  # 2003 CLNK
h77a = load_sas7bdat(spark, "C:/MEPS/h77a.sas7bdat")    # 2003 PMED
h79 = load_sas7bdat(spark, "C:/MEPS/h79.sas7bdat")      # 2003 FYC

# ---------------------------------------------------------------------------
# Identify conditions of interest: Diabetes (ICD9=250) and Heart Disease (ICD9=410-414)
# ---------------------------------------------------------------------------
cond_diabetes = (
    h78
    .filter(F.col("ICD9CODX").startswith("250"))
    .select("CONDIDX", "DUPERSID")
    .withColumn("CONDITION", F.lit("Diabetes"))
)

cond_heart = (
    h78
    .filter(
        F.col("ICD9CODX").startswith("410") |
        F.col("ICD9CODX").startswith("411") |
        F.col("ICD9CODX").startswith("412") |
        F.col("ICD9CODX").startswith("413") |
        F.col("ICD9CODX").startswith("414")
    )
    .select("CONDIDX", "DUPERSID")
    .withColumn("CONDITION", F.lit("Heart Disease"))
)

conditions = cond_diabetes.unionByName(cond_heart)

# ---------------------------------------------------------------------------
# Link conditions to events via CLNK
# Filter CLNK to only prescribed medicines (EVNTIDX ending in specific patterns)
# ---------------------------------------------------------------------------
clnk_pmed = clnk.filter(
    F.substring(F.col("EVNTIDX"), -2, 2) == "PM"
)

# Merge conditions with CLNK to get PMED event IDs
cond_pmed_link = (
    conditions
    .join(clnk_pmed, on=["CONDIDX", "DUPERSID"], how="inner")
    .select("EVNTIDX", "DUPERSID", "CONDITION", "CONDIDX")
)

print("=== Condition-PMED Links ===")
cond_pmed_link.groupBy("CONDITION").count().orderBy("CONDITION").show()

# ---------------------------------------------------------------------------
# Handle de-duplication: When a fill is linked to multiple conditions
# Count number of conditions linked to each fill
# ---------------------------------------------------------------------------
fill_cond_count = (
    cond_pmed_link
    .groupBy("EVNTIDX", "DUPERSID")
    .agg(F.countDistinct("CONDIDX").alias("N_COND_LINKED"))
)

# Merge with PMED data and allocate expenditures proportionally
pmed_data = (
    h77a
    .withColumnRenamed("LINKIDX", "EVNTIDX")
    .select("EVNTIDX", "DUPERSID", "RXXP03X", "RXSF03X")
)

cond_pmed_exp = (
    cond_pmed_link
    .join(pmed_data, on=["EVNTIDX", "DUPERSID"], how="inner")
    .join(fill_cond_count, on=["EVNTIDX", "DUPERSID"], how="left")
    # Allocate expenditures proportionally when linked to multiple conditions
    .withColumn("ALLOC_RXXP", F.col("RXXP03X") / F.col("N_COND_LINKED"))
    .withColumn("ALLOC_RXSF", F.col("RXSF03X") / F.col("N_COND_LINKED"))
)

# ---------------------------------------------------------------------------
# Aggregate to person-condition level
# ---------------------------------------------------------------------------
person_cond_exp = (
    cond_pmed_exp
    .groupBy("DUPERSID", "CONDITION")
    .agg(
        F.sum("ALLOC_RXXP").alias("PMED_TOT_EXP"),
        F.sum("ALLOC_RXSF").alias("PMED_OOP_EXP"),
        F.countDistinct("EVNTIDX").alias("N_FILLS")
    )
)

# Pivot to get separate columns for each condition
person_diab_exp = (
    person_cond_exp
    .filter(F.col("CONDITION") == "Diabetes")
    .withColumnRenamed("PMED_TOT_EXP", "DIAB_PMED_TOT")
    .withColumnRenamed("PMED_OOP_EXP", "DIAB_PMED_OOP")
    .withColumnRenamed("N_FILLS", "DIAB_N_FILLS")
    .select("DUPERSID", "DIAB_PMED_TOT", "DIAB_PMED_OOP", "DIAB_N_FILLS")
)

person_heart_exp = (
    person_cond_exp
    .filter(F.col("CONDITION") == "Heart Disease")
    .withColumnRenamed("PMED_TOT_EXP", "HEART_PMED_TOT")
    .withColumnRenamed("PMED_OOP_EXP", "HEART_PMED_OOP")
    .withColumnRenamed("N_FILLS", "HEART_N_FILLS")
    .select("DUPERSID", "HEART_PMED_TOT", "HEART_PMED_OOP", "HEART_N_FILLS")
)

# ---------------------------------------------------------------------------
# Merge with Full-Year file
# ---------------------------------------------------------------------------
merged = (
    h79
    .filter(F.col("PERWT03F") > 0)
    .join(person_diab_exp, on="DUPERSID", how="left")
    .join(person_heart_exp, on="DUPERSID", how="left")
    .fillna({
        "DIAB_PMED_TOT": 0, "DIAB_PMED_OOP": 0, "DIAB_N_FILLS": 0,
        "HEART_PMED_TOT": 0, "HEART_PMED_OOP": 0, "HEART_N_FILLS": 0
    })
    .withColumn("HAS_DIAB_PMED",
        F.when(F.col("DIAB_PMED_TOT") > 0, 1).otherwise(0))
    .withColumn("HAS_HEART_PMED",
        F.when(F.col("HEART_PMED_TOT") > 0, 1).otherwise(0))
)

# ---------------------------------------------------------------------------
# Survey-weighted estimates: Diabetes PMED expenditures
# ---------------------------------------------------------------------------
print("=== Diabetes PMED Expenditures (Among Those with Diabetes PMEDs) ===")
diab_users = merged.filter(F.col("HAS_DIAB_PMED") == 1)
results_diab = survey_mean(
    diab_users,
    var_cols=["DIAB_PMED_TOT", "DIAB_PMED_OOP", "DIAB_N_FILLS"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F"
)
print(results_diab)

# ---------------------------------------------------------------------------
# Survey-weighted estimates: Heart Disease PMED expenditures
# ---------------------------------------------------------------------------
print("\n=== Heart Disease PMED Expenditures (Among Those with Heart PMEDs) ===")
heart_users = merged.filter(F.col("HAS_HEART_PMED") == 1)
results_heart = survey_mean(
    heart_users,
    var_cols=["HEART_PMED_TOT", "HEART_PMED_OOP", "HEART_N_FILLS"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F"
)
print(results_heart)

spark.stop()
