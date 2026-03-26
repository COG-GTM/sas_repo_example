"""
Exercise 2c: Narcotic Analgesics or Narcotic Analgesic Combos, 2018

Generates National Totals and Per-person Averages including:
  - Number of purchases (fills)
  - Total expenditures
  - Out-of-pocket payments
  - Third-party payments

Input files:
  - C:/MEPS/h209.sas7bdat (2018 Full-Year Consolidated)
  - C:/MEPS/h206a.sas7bdat (2018 Prescribed Medicines)

Replaces: SAS/workshop_exercises/exercise_2c/Exercise2c.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# Load prescribed medicines and filter to narcotic analgesics/combos
h206a = load_sas7bdat(spark, "C:/MEPS/h206a.sas7bdat")
drug = h206a.filter(F.col("TC1S1_1").isin(60, 191))

print("=== Sample PMED records, 2018 ===")
drug.select("DUPERSID", "RXRECIDX", "LINKIDX", "TC1S1_1",
            "RXXP18X", "RXSF18X").show(12, truncate=False)

# Sum to person-level
perdrug = (
    drug
    .groupBy("DUPERSID")
    .agg(
        F.sum("RXXP18X").alias("TOT"),
        F.sum("RXSF18X").alias("OOP"),
        F.count("*").alias("N_PHRCHASE"),
    )
    .withColumn("THIRD_PAYER", F.col("TOT") - F.col("OOP"))
)

# Merge to FY PUF
h209 = load_sas7bdat(spark, "C:/MEPS/h209.sas7bdat")
fy_base = h209.select("DUPERSID", "VARSTR", "VARPSU", "PERWT18F")

fy = (
    fy_base
    .join(perdrug, on="DUPERSID", how="left")
    .withColumn("SUBPOP",
        F.when(F.col("TOT").isNotNull(), "OnePlusNarcoticEtc").otherwise("OTHERS"))
    .fillna({"N_PHRCHASE": 0, "THIRD_PAYER": 0, "TOT": 0, "OOP": 0})
)

# Calculate estimates
print("=== Person-level estimates for narcotic analgesics, 2018 ===")
results = survey_mean(
    fy,
    var_cols=["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT18F",
    domain_col="SUBPOP",
)
print(results.to_string())

results_total = survey_total(
    fy,
    var_cols=["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT18F",
    domain_col="SUBPOP",
)
print(results_total.to_string())

spark.stop()
