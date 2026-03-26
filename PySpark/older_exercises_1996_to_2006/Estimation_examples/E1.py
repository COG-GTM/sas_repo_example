"""
E1: Person-Level Estimates with Domain Analysis (2001 Data)

This example shows how to compute person-level estimates for total healthcare
expenditures and the percent with an expense, overall and for specific
demographic subpopulations.

Migrated from: SAS/older_exercises_1996_to_2006/Estimation_examples/E1/E1.sas
Input file: h60.sas7bdat (2001 Full-Year Population Characteristics File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# ---------------------------------------------------------------------------
# Load 2001 Full-Year Consolidated Data File (HC-060)
# ---------------------------------------------------------------------------
h60 = load_sas7bdat(spark, "C:/MEPS/h60.sas7bdat")

# ---------------------------------------------------------------------------
# Create analytic variables
# ---------------------------------------------------------------------------
meps = (
    h60
    # AGE: use best available age variable
    .withColumn("AGE",
        F.when(F.col("AGE01X") >= 0, F.col("AGE01X"))
         .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
         .when(F.col("AGE31X") >= 0, F.col("AGE31X"))
         .otherwise(-1)
    )
    # AGE category
    .withColumn("AGECAT",
        F.when((F.col("AGE") >= 0) & (F.col("AGE") <= 64), 1)
         .when(F.col("AGE") >= 65, 2)
         .otherwise(-1)
    )
    .withColumn("AGECAT_label",
        F.when(F.col("AGECAT") == 1, "Under 65")
         .when(F.col("AGECAT") == 2, "65+")
         .otherwise("Unknown")
    )
    # SEX label
    .withColumn("SEX_label",
        F.when(F.col("SEX") == 1, "1 Male")
         .when(F.col("SEX") == 2, "2 Female")
         .otherwise("Unknown")
    )
    # Total expenditures
    .withColumn("TOTEXP", F.col("TOTEXP01"))
    # ANY_EXP: dichotomous variable (0/100) for percentage output
    .withColumn("ANY_EXP",
        F.when(F.col("TOTEXP01") > 0, 100).otherwise(0)
    )
)

# ---------------------------------------------------------------------------
# QC: Frequency checks
# ---------------------------------------------------------------------------
print("=== Age Category Distribution ===")
meps.groupBy("AGECAT_label").count().orderBy("AGECAT_label").show()

print("=== Sex Distribution ===")
meps.groupBy("SEX_label").count().orderBy("SEX_label").show()

# ---------------------------------------------------------------------------
# Overall estimates: Mean total expenditures and percent with expense
# ---------------------------------------------------------------------------
print("=== Overall: Mean Total Expenditures and Percent with Expense ===")
results_overall = survey_mean(
    meps,
    var_cols=["TOTEXP", "ANY_EXP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT01F"
)
print(results_overall)

# ---------------------------------------------------------------------------
# Domain: Mean total expenditures by age category
# ---------------------------------------------------------------------------
print("\n=== Mean Total Expenditures by Age Category ===")
results_age = survey_mean(
    meps,
    var_cols=["TOTEXP", "ANY_EXP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT01F",
    domain_col="AGECAT_label"
)
print(results_age)

# ---------------------------------------------------------------------------
# Domain: Mean total expenditures by sex
# ---------------------------------------------------------------------------
print("\n=== Mean Total Expenditures by Sex ===")
results_sex = survey_mean(
    meps,
    var_cols=["TOTEXP", "ANY_EXP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT01F",
    domain_col="SEX_label"
)
print(results_sex)

spark.stop()
