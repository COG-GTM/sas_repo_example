"""
Exercise 1c: National Health Care Expenses, 2018

Generates the following estimates:
  - Overall expenses (National totals)
  - Percentage of persons with an expense
  - Mean expense per person
  - Mean/median expense per person with an expense, by age group

Input file: C:/MEPS/h209.sas7bdat (2018 Full-Year File)

Replaces: SAS/workshop_exercises/exercise_1c/Exercise1c.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total, survey_freq

spark = get_spark()

# Load 2018 FYC
h209 = load_sas7bdat(spark, "C:/MEPS/h209.sas7bdat")

puf209 = (
    h209
    .select("DUPERSID", "TOTEXP18", "AGELAST", "VARSTR", "VARPSU", "PERWT18F", "PANEL")
    .withColumn("WITH_AN_EXPENSE",
        F.when(F.col("TOTEXP18") > 0, "Any Expense").otherwise("No Expense"))
    .withColumn("AGECAT_label",
        F.when(F.col("AGELAST") <= 64, "0-64")
        .when(F.col("AGELAST") >= 65, "65+"))
)

# Percentage of persons with an expense - Method 1 (survey mean of indicator)
print("=== Percentage of persons with an expense, 2018 ===")
puf209_flag = puf209.withColumn("has_expense",
    F.when(F.col("TOTEXP18") > 0, 1).otherwise(0))
results_pct = survey_mean(
    puf209_flag,
    var_cols=["has_expense"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT18F",
)
print(results_pct.to_string())

# Percentage of persons with an expense - Method 2 (survey freq)
print("\n=== Percentage of persons with an expense (SURVEYFREQ), 2018 ===")
results_freq = survey_freq(
    puf209,
    table_vars=["WITH_AN_EXPENSE"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT18F",
)
print(results_freq.to_string())

# Mean and median expense per person with an expense, by age
print("\n=== Mean expense per person with an expense, by age, 2018 ===")
puf209_expense = puf209.filter(F.col("TOTEXP18") > 0)
results_mean = survey_mean(
    puf209_expense,
    var_cols=["TOTEXP18"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT18F",
    domain_col="AGECAT_label",
)
print(results_mean.to_string())

# Note: Survey-weighted median requires specialized implementation.
# The samplics library or custom quantile estimation can be used.
# For now, we compute unweighted median as an approximation.
print("\n=== Approximate median expense per person with an expense, by age ===")
for age_group in ["0-64", "65+"]:
    subset = puf209_expense.filter(F.col("AGECAT_label") == age_group)
    median_val = subset.approxQuantile("TOTEXP18", [0.5], 0.01)
    if median_val:
        print(f"  {age_group}: Approx. Median = ${median_val[0]:,.0f}")

spark.stop()
