"""
Exercise 2a: Antipsychotic Purchases and Expenses, 2015

Generates selected estimates:
  (1) Total expense for antipsychotics
  (2) Total number of purchases of antipsychotics
  (3) Total number of persons purchasing one or more antipsychotics
  (4) Average total, out of pocket, and third party payer expense per person

Input files:
  - C:/MEPS/h181.sas7bdat (2015 Full-Year Consolidated)
  - C:/MEPS/h178a.sas7bdat (2015 Prescribed Medicines)

Replaces: SAS/workshop_exercises/exercise_2a/Exercise2a.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# 1) Identify antipsychotic drugs using therapeutic classification codes
h178a = load_sas7bdat(spark, "C:/MEPS/h178a.sas7bdat")
drug = h178a.filter((F.col("TC1") == 242) & (F.col("TC1S1") == 251))

print("=== Sample PMED records with antipsychotic drugs ===")
drug.select("DUPERSID", "RXRECIDX", "LINKIDX", "TC1", "TC1S1",
            "RXXP15X", "RXSF15X").show(30, truncate=False)

# 2) Sum data to person-level
perdrug = (
    drug
    .groupBy("DUPERSID")
    .agg(
        F.sum("RXXP15X").alias("TOT"),
        F.sum("RXSF15X").alias("OOP"),
        F.count("*").alias("N_PHRCHASE"),
    )
    .withColumn("THIRD_PAYER", F.col("TOT") - F.col("OOP"))
)

# 3) Merge person-level expenditures to FY PUF
h181 = load_sas7bdat(spark, "C:/MEPS/h181.sas7bdat")
fy_base = h181.select("DUPERSID", "VARSTR", "VARPSU", "PERWT15F")

fy = (
    fy_base
    .join(perdrug, on="DUPERSID", how="left")
    .withColumn("SUB",
        F.when(F.col("TOT").isNotNull(), 1).otherwise(2))
    .fillna({"N_PHRCHASE": 0, "THIRD_PAYER": 0, "TOT": 0, "OOP": 0})
)

# QC: Supporting crosstabs
print("=== Supporting crosstabs ===")
fy.groupBy("SUB").count().show()

# 4) Calculate estimates on expenditures and use
print("=== Person-level estimates for antipsychotic drugs, 2015 ===")
results = survey_mean(
    fy,
    var_cols=["TOT", "N_PHRCHASE", "OOP", "THIRD_PAYER"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
    domain_col="SUB",
)
print(results.to_string())

results_total = survey_total(
    fy,
    var_cols=["TOT", "N_PHRCHASE", "OOP", "THIRD_PAYER"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
    domain_col="SUB",
)
print(results_total.to_string())

spark.stop()
