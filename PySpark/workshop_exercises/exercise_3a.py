"""
Exercise 3a: Estimates on Use and Expenditures for Persons with Diabetes, 2015

Illustrates how to:
  - Identify persons with a condition (diabetes, CCS codes 049/050)
  - Calculate estimates on use and expenditures for persons with the condition

Input files:
  - C:/MEPS/h180.sas7bdat (2015 Condition PUF)
  - C:/MEPS/h181.sas7bdat (2015 Full-Year PUF)

Replaces: SAS/workshop_exercises/exercise_3a/Exercise3a.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# 1) Pull out conditions with diabetes (CCS codes 049, 050)
h180 = load_sas7bdat(spark, "C:/MEPS/h180.sas7bdat")
diab = h180.filter(F.col("CCCODEX").isin("049", "050"))

print("=== Check CCS codes for diabetic conditions ===")
diab.groupBy("CCCODEX").count().show()

# 2) Identify persons who reported diabetes (unique persons)
diabpers = diab.select("DUPERSID").distinct()

# 3) Create flag for persons with diabetes in 2015 FY data
h181 = load_sas7bdat(spark, "C:/MEPS/h181.sas7bdat")

fy1 = (
    h181
    .join(diabpers, on="DUPERSID", how="left")
    .withColumn("DIABPERS",
        F.when(diabpers["DUPERSID"].isNotNull(), 1).otherwise(2))
)

# Since the left join approach may not work cleanly, use a flag approach
diabpers_flag = diabpers.withColumn("_has_diabetes", F.lit(1))
fy1 = (
    h181
    .join(diabpers_flag, on="DUPERSID", how="left")
    .withColumn("DIABPERS",
        F.when(F.col("_has_diabetes") == 1, 1).otherwise(2))
    .drop("_has_diabetes")
)

# QC: Unweighted counts
print("=== Unweighted # of persons who reported diabetes, 2015 ===")
fy1.groupBy("DIABPERS").count().show()
fy1.groupBy("DIABPERS", "SEX").count().orderBy("DIABPERS", "SEX").show()

# 4) Calculate estimates for persons who reported diabetes
print("=== Estimates for persons who reported diabetes, 2015 ===")
fy1 = fy1.withColumn("SEX_label",
    F.when(F.col("SEX") == 1, "MALE")
    .when(F.col("SEX") == 2, "FEMALE"))

# Overall estimates for diabetics
results = survey_mean(
    fy1,
    var_cols=["TOTEXP15", "TOTSLF15", "OBTOTV15"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
    domain_col="DIABPERS",
)
print(results.to_string())

# Estimates by sex for diabetics
fy1_diab = fy1.filter(F.col("DIABPERS") == 1)
results_sex = survey_mean(
    fy1_diab,
    var_cols=["TOTEXP15", "TOTSLF15", "OBTOTV15"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
    domain_col="SEX_label",
)
print(results_sex.to_string())

results_total = survey_total(
    fy1,
    var_cols=["TOTEXP15", "TOTSLF15", "OBTOTV15"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
    domain_col="DIABPERS",
)
print(results_total.to_string())

spark.stop()
