"""
Exercise 4c: Pool MEPS Data Files (2017-2018) with CAPI Redesign Discontinuity

Illustrates pooling with a discontinuity from the 2018 CAPI re-design.
Calculates for the civilian noninstitutionalized population:
  - Percentage of people with Joint Pain / Arthritis
  - Average expenditures per person, by Joint Pain status

Input files:
  - C:/MEPS/h201.sas7bdat (2017 Full-Year File)
  - C:/MEPS/h209.sas7bdat (2018 Full-Year File)

Replaces: SAS/workshop_exercises/exercise_4c/Exercise4c.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_freq

spark = get_spark()

# Load 2017 data
h201 = load_sas7bdat(spark, "C:/MEPS/h201.sas7bdat")
meps_2017 = (
    h201
    .select("VARSTR", "VARPSU", "PERWT17F", "AGELAST", "ARTHDX", "JTPAIN31",
            "TOTEXP17", "TOTSLF17")
    .withColumnRenamed("TOTEXP17", "TOTEXP")
    .withColumnRenamed("TOTSLF17", "TOTSLF")
    .withColumnRenamed("JTPAIN31", "JTPAIN")
    .withColumn("PERWTF", F.col("PERWT17F") / 2)
    # Create subpop and joint_pain variables
    .withColumn("SPOP",
        F.when(
            (F.col("AGELAST") >= 18) &
            ~((F.col("ARTHDX") <= 0) & (F.col("JTPAIN") < 0)), 1
        ).otherwise(2))
    .withColumn("JOINT_PAIN",
        F.when(
            (F.col("SPOP") == 1) &
            ((F.col("ARTHDX") == 1) | (F.col("JTPAIN") == 1)), 1
        ).when(F.col("SPOP") == 1, 2))
)

# Load 2018 data
h209 = load_sas7bdat(spark, "C:/MEPS/h209.sas7bdat")
meps_2018 = (
    h209
    .select("VARSTR", "VARPSU", "PERWT18F", "AGELAST", "ARTHDX", "JTPAIN31_M18",
            "TOTEXP18", "TOTSLF18")
    .withColumnRenamed("TOTEXP18", "TOTEXP")
    .withColumnRenamed("TOTSLF18", "TOTSLF")
    .withColumnRenamed("JTPAIN31_M18", "JTPAIN")
    .withColumn("PERWTF", F.col("PERWT18F") / 2)
    .withColumn("SPOP",
        F.when(
            (F.col("AGELAST") >= 18) &
            ~((F.col("ARTHDX") <= 0) & (F.col("JTPAIN") < 0)), 1
        ).otherwise(2))
    .withColumn("JOINT_PAIN",
        F.when(
            (F.col("SPOP") == 1) &
            ((F.col("ARTHDX") == 1) | (F.col("JTPAIN") == 1)), 1
        ).when(F.col("SPOP") == 1, 2))
)

# Concatenate 2017 and 2018
meps_1718 = meps_2017.unionByName(meps_2018, allowMissingColumns=True)

# Add label
meps_1718 = meps_1718.withColumn("JOINT_PAIN_label",
    F.when(F.col("JOINT_PAIN") == 1, "Yes")
    .when(F.col("JOINT_PAIN") == 2, "No"))

# QC
print("=== QC: Joint pain by SPOP ===")
meps_1718.groupBy("SPOP", "JOINT_PAIN_label").count().orderBy("SPOP", "JOINT_PAIN_label").show()

# Percentage of people with joint pain (subpop = adults 18+)
print("=== Percentage with joint pain, 2017-18 pooled ===")
meps_sub = meps_1718.filter(F.col("SPOP") == 1)
results_freq = survey_freq(
    meps_sub,
    table_vars=["JOINT_PAIN_label"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWTF",
)
print(results_freq.to_string())

# Average expenditures by joint pain status
print("\n=== Mean expenditures by joint pain status, 2017-18 pooled ===")
results_exp = survey_mean(
    meps_sub,
    var_cols=["TOTEXP", "TOTSLF"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWTF",
    domain_col="JOINT_PAIN_label",
)
print(results_exp.to_string())

spark.stop()
