"""
Exercise 2b: Narcotic Analgesics or Narcotic Analgesic Combos, 2016

Generates selected estimates:
  (1) Total expense for narcotic analgesics
  (2) Total number of purchases
  (3) Total number of persons purchasing 1+
  (4) Average total, OOP, and third party payer expense per person

Input files:
  - C:/MEPS/h192.sas7bdat (2016 Full-Year Consolidated)
  - C:/MEPS/h188a.sas7bdat (2016 Prescribed Medicines)

Replaces: SAS/workshop_exercises/exercise_2b/Exercise2b.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# 1) Identify narcotic analgesics using TC codes
h188a = load_sas7bdat(spark, "C:/MEPS/h188a.sas7bdat")
drug = h188a.filter(F.col("TC1S1_1").isin(60, 191))

print("=== Sample PMED records ===")
drug.select("DUPERSID", "RXRECIDX", "LINKIDX", "TC1S1_1",
            "RXXP16X", "RXSF16X").show(30, truncate=False)

# 2) Sum to person-level
perdrug = (
    drug
    .groupBy("DUPERSID")
    .agg(
        F.sum("RXXP16X").alias("TOT"),
        F.sum("RXSF16X").alias("OOP"),
        F.count("*").alias("N_PHRCHASE"),
    )
    .withColumn("THIRD_PAYER", F.col("TOT") - F.col("OOP"))
)

# 3) Merge to FY PUF
h192 = load_sas7bdat(spark, "C:/MEPS/h192.sas7bdat")
fy_base = h192.select("DUPERSID", "VARSTR", "VARPSU", "PERWT16F")

fy = (
    fy_base
    .join(perdrug, on="DUPERSID", how="left")
    .withColumn("SUB",
        F.when(F.col("TOT").isNotNull(), 1).otherwise(2))
    .fillna({"N_PHRCHASE": 0, "THIRD_PAYER": 0, "TOT": 0, "OOP": 0})
)

# 4) Calculate estimates
print("=== Person-level estimates for narcotic analgesics, 2016 ===")
results = survey_mean(
    fy,
    var_cols=["TOT", "N_PHRCHASE", "OOP", "THIRD_PAYER"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
    domain_col="SUB",
)
print(results.to_string())

results_total = survey_total(
    fy,
    var_cols=["TOT", "N_PHRCHASE", "OOP", "THIRD_PAYER"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
    domain_col="SUB",
)
print(results_total.to_string())

spark.stop()
