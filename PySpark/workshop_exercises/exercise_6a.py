"""
Exercise 6a: Logistic Regression - Flu Shot, 2018

Includes:
  - Percentage of people with a flu shot (civilian noninstitutionalized population)
  - Logistic regression: demographic factors associated with receiving a flu shot

Input file: C:/MEPS/h209v9.sas7bdat (2018 Full-Year File)

Replaces: SAS/workshop_exercises/exercise_6a/Exercise6.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_logistic

spark = get_spark()

# Load 2018 FYC
h209 = load_sas7bdat(spark, "C:/MEPS/h209v9.sas7bdat")

meps_2018 = (
    h209
    .select("VARSTR", "VARPSU", "PERWT18F", "SAQWT18F", "ADFLST42",
            "AGELAST", "RACETHX", "POVCAT18", "INSCOV18", "SEX")
    # Create flushot variable: 1=Yes, 0=No, null=missing
    .withColumn("FLUSHOT",
        F.when(F.col("ADFLST42") == 1, 1)
        .when(F.col("ADFLST42") == 2, 0))
    # Label variables
    .withColumn("AGE18P",
        F.when(F.col("AGELAST") >= 18, "18+").otherwise("0-17"))
    .withColumn("AGE_label",
        F.when((F.col("AGELAST") >= 18) & (F.col("AGELAST") <= 34), "18-34")
        .when((F.col("AGELAST") >= 35) & (F.col("AGELAST") <= 64), "35-64")
        .when(F.col("AGELAST") >= 65, "65+"))
    .withColumn("SEX_label",
        F.when(F.col("SEX") == 1, "Male")
        .when(F.col("SEX") == 2, "Female"))
    .withColumn("RACETHX_label",
        F.when(F.col("RACETHX") == 1, "Hispanic")
        .when(F.col("RACETHX") == 2, "NH White only")
        .when(F.col("RACETHX") == 3, "NH Black only")
        .when(F.col("RACETHX") == 4, "NH Asian only")
        .when(F.col("RACETHX") == 5, "NH Other etc"))
    .withColumn("INSCOV18_label",
        F.when(F.col("INSCOV18") == 1, "Any Private")
        .when(F.col("INSCOV18") == 2, "Public Only")
        .when(F.col("INSCOV18") == 3, "Uninsured"))
)

# Percentage of adults with a flu shot (using SAQ weight)
print("=== Percentage of adults 18+ with flu shot, 2018 ===")
adults = meps_2018.filter(F.col("AGELAST") >= 18)
results = survey_mean(
    adults,
    var_cols=["FLUSHOT"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="SAQWT18F",
)
print(results.to_string())

# Logistic regression: factors associated with flu shot
print("\n=== Logistic Regression: Flu Shot ===")
print("Model: flushot ~ agelast + sex + racethx + inscov18")
print("Reference categories: sex=Male, racethx=Hispanic, inscov18=Any Private")

logit_results = survey_logistic(
    meps_2018.filter(F.col("AGELAST") >= 18),
    model_formula="FLUSHOT ~ AGELAST + SEX_label + RACETHX_label + INSCOV18_label",
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="SAQWT18F",
    class_vars=["SEX_label", "RACETHX_label", "INSCOV18_label"],
)
print(logit_results)

spark.stop()
