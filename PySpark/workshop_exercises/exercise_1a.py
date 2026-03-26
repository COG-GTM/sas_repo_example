"""
Exercise 1a: National Health Care Expenses, 2016

Generates the following estimates:
  (1) Overall expenses
  (2) Percentage of persons with an expense
  (3) Mean expense per person with an expense

Input file: C:/MEPS/h192.sas7bdat (2016 Full-Year File)

Replaces: SAS/workshop_exercises/exercise_1a/Exercise1a.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# Load 2016 FYC
h192 = load_sas7bdat(spark, "C:/MEPS/h192.sas7bdat")

# Keep needed variables and create derived variables
puf192 = (
    h192
    .select("DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
            "VARSTR", "VARPSU", "PERWT16F")
    .withColumn("TOTAL", F.col("TOTEXP16"))
    # Create flag for persons with an expense
    .withColumn("X_ANYSVCE",
        F.when(F.col("TOTAL") > 0, 1).otherwise(0))
    # Create age summary variable
    .withColumn("AGE",
        F.when(F.col("AGE16X") >= 0, F.col("AGE16X"))
        .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
        .when(F.col("AGE31X") >= 0, F.col("AGE31X")))
    # Create age category
    .withColumn("AGECAT",
        F.when((F.col("AGE") >= 0) & (F.col("AGE") <= 64), 1)
        .when(F.col("AGE") > 64, 2))
    .withColumn("AGECAT_label",
        F.when(F.col("AGECAT") == 1, "0-64")
        .when(F.col("AGECAT") == 2, "65+"))
)

# QC: Supporting crosstabs
print("=== Supporting crosstabs ===")
puf192.groupBy("X_ANYSVCE").count().show()

# (1) & (2) Percentage of persons with an expense & Overall expenses
print("=== Percentage of persons with an expense & Overall expenses ===")
results_overall = survey_mean(
    puf192,
    var_cols=["X_ANYSVCE", "TOTAL"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
)
print(results_overall.to_string())

results_total = survey_total(
    puf192,
    var_cols=["X_ANYSVCE", "TOTAL"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
)
print(results_total.to_string())

# (3) Mean expense per person WITH an expense, overall and by age group
# Use DOMAIN analysis: X_ANYSVCE('1') and X_ANYSVCE('1')*AGECAT
print("\n=== Mean expense per person with an expense, by age group ===")
puf192_domain = puf192.withColumn("domain",
    F.when(F.col("X_ANYSVCE") == 1, 1).otherwise(0))

results_domain = survey_mean(
    puf192_domain,
    var_cols=["TOTAL"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
    domain_col="AGECAT_label",
)
print(results_domain.to_string())

spark.stop()
