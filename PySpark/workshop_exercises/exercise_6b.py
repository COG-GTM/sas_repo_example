"""
Exercise 6b: Logistic Regression - Delayed Care due to COVID, 2020

Includes 3 logistic regression models with separate dependent variables:
  - cvdlayca53: Delayed medical care for COVID
  - cvdlaydn53: Delayed dental care for COVID
  - cvdlaypm53: Delayed prescribed medicines for COVID

Covariates: age, gender, race/ethnicity, health insurance, region

Also estimates proportion of persons with delayed care events.

Input file: C:/MEPS/h224.sas7bdat (2020 Full-Year File)

Replaces: SAS/workshop_exercises/exercise_6b/exercise6.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_logistic

spark = get_spark()

# Load 2020 FYC
h224 = load_sas7bdat(spark, "C:/MEPS/h224.sas7bdat")

meps_2020 = (
    h224
    .select("VARSTR", "VARPSU", "PERWT20F", "CVDLAYCA53", "CVDLAYPM53",
            "CVDLAYDN53", "AGELAST", "SEX", "RACETHX", "POVCAT20",
            "INSCOV20", "REGION53")
    # Recode region (region53=-1 -> missing)
    .withColumn("REGION",
        F.when(F.col("REGION53") == -1, None).otherwise(F.col("REGION53")))
    # Recode delayed care variables: 1->1 (yes), 2->0 (no), negative->null
    .withColumn("DELAYED_CARE_MED",
        F.when(F.col("CVDLAYCA53") == 1, 1)
        .when(F.col("CVDLAYCA53") == 2, 0))
    .withColumn("DELAYED_CARE_DENTAL",
        F.when(F.col("CVDLAYDN53") == 1, 1)
        .when(F.col("CVDLAYDN53") == 2, 0))
    .withColumn("DELAYED_CARE_PMEDS",
        F.when(F.col("CVDLAYPM53") == 1, 1)
        .when(F.col("CVDLAYPM53") == 2, 0))
    # Label variables for regression
    .withColumn("SEX_label",
        F.when(F.col("SEX") == 1, "1. male")
        .when(F.col("SEX") == 2, "2. female"))
    .withColumn("RACETHX_label",
        F.when(F.col("RACETHX") == 1, "1. hispanic")
        .when(F.col("RACETHX") == 2, "2. nh white only")
        .when(F.col("RACETHX") == 3, "3. nh black only")
        .when(F.col("RACETHX") == 4, "4. nh asian only")
        .when(F.col("RACETHX") == 5, "5. nh other etc"))
    .withColumn("INSCOV20_label",
        F.when(F.col("INSCOV20") == 1, "1. any private")
        .when(F.col("INSCOV20") == 2, "2. public only")
        .when(F.col("INSCOV20") == 3, "3. uninsured"))
    .withColumn("REGION_label",
        F.when(F.col("REGION") == 1, "1. northeast")
        .when(F.col("REGION") == 2, "2. midwest")
        .when(F.col("REGION") == 3, "3. south")
        .when(F.col("REGION") == 4, "4. west"))
)

# Proportion of persons with delayed care events
print("=== Proportion of persons with delayed care events, 2020 ===")
results = survey_mean(
    meps_2020,
    var_cols=["DELAYED_CARE_MED", "DELAYED_CARE_DENTAL", "DELAYED_CARE_PMEDS"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT20F",
)
print(results.to_string())

# Model covariates (shared across all 3 models)
covariates = "AGELAST + SEX_label + RACETHX_label + INSCOV20_label + REGION_label"
class_vars = ["SEX_label", "RACETHX_label", "INSCOV20_label", "REGION_label"]

# Model 1: Delayed medical care
print("\n=== Logistic Regression: Delayed Medical Care ===")
print(f"Model: DELAYED_CARE_MED ~ {covariates}")
print("Reference: sex=1. male, racethx=1. hispanic, inscov20=1. any private, region=1. northeast")
logit1 = survey_logistic(
    meps_2020,
    model_formula=f"DELAYED_CARE_MED ~ {covariates}",
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT20F",
    class_vars=class_vars,
)
print(logit1)

# Model 2: Delayed dental care
print("\n=== Logistic Regression: Delayed Dental Care ===")
print(f"Model: DELAYED_CARE_DENTAL ~ {covariates}")
logit2 = survey_logistic(
    meps_2020,
    model_formula=f"DELAYED_CARE_DENTAL ~ {covariates}",
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT20F",
    class_vars=class_vars,
)
print(logit2)

# Model 3: Delayed prescribed medicines
print("\n=== Logistic Regression: Delayed Prescribed Medicines ===")
print(f"Model: DELAYED_CARE_PMEDS ~ {covariates}")
logit3 = survey_logistic(
    meps_2020,
    model_formula=f"DELAYED_CARE_PMEDS ~ {covariates}",
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT20F",
    class_vars=class_vars,
)
print(logit3)

spark.stop()
