"""
Exercise 4d: Pool MEPS Data Files (2017-2019) with Pooled Variance Linkage

Illustrates pooling 2017, 2018, and 2019 data with the pooled linkage
variance estimation file (h36u19) for correct standard errors.

Calculates:
  - Percentage of people with Joint Pain / Arthritis (JTPAIN, ARTHDX)
  - Average expenditures per person, by Joint Pain status (TOTEXP, TOTSLF)

Input files:
  - C:/MEPS/h201v9.sas7bdat (2017 Full-Year File)
  - C:/MEPS/h209v9.sas7bdat (2018 Full-Year File)
  - C:/MEPS/h216.sas7bdat   (2019 Full-Year File)
  - C:/MEPS/h36u19.sas7bdat (1996-2019 Pooled Linkage Variance File)

Replaces: SAS/workshop_exercises/exercise_4d/Exercise4.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_freq

spark = get_spark()

# --- Load and prepare 2017 data ---
h201 = load_sas7bdat(spark, "C:/MEPS/h201v9.sas7bdat")
meps_2017 = (
    h201
    .select("DUPERSID", "PANEL", "VARSTR", "VARPSU", "PERWT17F",
            "AGELAST", "ARTHDX", "JTPAIN31", "TOTEXP17", "TOTSLF17")
    .withColumnRenamed("TOTEXP17", "TOTEXP")
    .withColumnRenamed("TOTSLF17", "TOTSLF")
    .withColumn("YEAR", F.lit(2017))
    .withColumn("PERWTF", F.col("PERWT17F") / 3)
    # For 2017: 8-character DUPERSID needs panel prefix for 10-character format
    .withColumn("DUPERSID",
        F.concat(F.lpad(F.col("PANEL").cast("string"), 2, "0"), F.col("DUPERSID")))
    .withColumn("SPOP",
        F.when(
            (F.col("AGELAST") >= 18) &
            ~((F.col("ARTHDX") <= 0) & (F.col("JTPAIN31") < 0)), 1
        ).otherwise(0))
    .withColumn("JOINT_PAIN",
        F.when(
            (F.col("SPOP") == 1) &
            ((F.col("ARTHDX") == 1) | (F.col("JTPAIN31") == 1)), 1
        ).when(F.col("SPOP") == 1, 2))
)

# --- Load and prepare 2018 data ---
h209 = load_sas7bdat(spark, "C:/MEPS/h209v9.sas7bdat")
meps_2018 = (
    h209
    .select("DUPERSID", "PANEL", "VARSTR", "VARPSU", "PERWT18F",
            "AGELAST", "ARTHDX", "JTPAIN31_M18", "TOTEXP18", "TOTSLF18")
    .withColumnRenamed("TOTEXP18", "TOTEXP")
    .withColumnRenamed("TOTSLF18", "TOTSLF")
    .withColumn("YEAR", F.lit(2018))
    .withColumn("PERWTF", F.col("PERWT18F") / 3)
    .withColumn("SPOP",
        F.when(
            (F.col("AGELAST") >= 18) &
            ~((F.col("ARTHDX") < 0) & (F.col("JTPAIN31_M18") < 0)), 1
        ).otherwise(0))
    .withColumn("JOINT_PAIN",
        F.when(
            (F.col("SPOP") == 1) &
            ((F.col("ARTHDX") == 1) | (F.col("JTPAIN31_M18") == 1)), 1
        ).when(F.col("SPOP") == 1, 2))
)

# --- Load and prepare 2019 data ---
h216 = load_sas7bdat(spark, "C:/MEPS/h216.sas7bdat")
meps_2019 = (
    h216
    .select("DUPERSID", "PANEL", "VARSTR", "VARPSU", "PERWT19F",
            "AGELAST", "ARTHDX", "JTPAIN31_M18", "TOTEXP19", "TOTSLF19")
    .withColumnRenamed("TOTEXP19", "TOTEXP")
    .withColumnRenamed("TOTSLF19", "TOTSLF")
    .withColumn("YEAR", F.lit(2019))
    .withColumn("PERWTF", F.col("PERWT19F") / 3)
    .withColumn("SPOP",
        F.when(
            (F.col("AGELAST") >= 18) &
            ~((F.col("ARTHDX") < 0) & (F.col("JTPAIN31_M18") < 0)), 1
        ).otherwise(0))
    .withColumn("JOINT_PAIN",
        F.when(
            (F.col("SPOP") == 1) &
            ((F.col("ARTHDX") == 1) | (F.col("JTPAIN31_M18") == 1)), 1
        ).when(F.col("SPOP") == 1, 2))
)

# --- Concatenate all three years ---
common_cols = ["DUPERSID", "PANEL", "VARSTR", "VARPSU", "PERWTF",
               "AGELAST", "TOTEXP", "TOTSLF", "YEAR", "SPOP", "JOINT_PAIN"]
meps_171819 = (
    meps_2017.select(common_cols)
    .unionByName(meps_2018.select(common_cols))
    .unionByName(meps_2019.select(common_cols))
    .orderBy("DUPERSID")
)

# --- Load and prepare pooled linkage variance estimation file ---
h36u19 = load_sas7bdat(spark, "C:/MEPS/h36u19.sas7bdat")

# Handle 8-char to 10-char DUPERSID conversion for pre-2018 panels
vs_file = (
    h36u19
    .withColumn("DUPERSID",
        F.when(
            F.length(F.trim(F.col("DUPERSID"))) == 8,
            F.concat(F.lpad(F.col("PANEL").cast("string"), 2, "0"), F.col("DUPERSID"))
        ).otherwise(F.col("DUPERSID")))
    .filter(F.col("PANEL").isin(21, 22, 23, 24))
    .dropDuplicates(["DUPERSID"])
)

# --- Merge pooled data with variance file ---
meps_merged = meps_171819.join(vs_file.select("DUPERSID", "STRA9619", "PSU9619"),
                                on="DUPERSID", how="left")

# Add labels
meps_merged = meps_merged.withColumn("JOINT_PAIN_label",
    F.when(F.col("JOINT_PAIN") == 1, "Yes")
    .when(F.col("JOINT_PAIN") == 2, "No"))

# Filter to valid observations
meps_valid = meps_merged.filter(
    (F.col("SPOP") == 1) &
    (F.col("PERWTF") > 0) &
    F.col("STRA9619").isNotNull() &
    F.col("PSU9619").isNotNull()
)

# --- Pooled estimates using linkage variance file PSU/Strata ---
print("=== Pooled estimates for MEPS 2017-19 ===")
print("=== Percentage with joint pain (adults 18+) ===")
results_freq = survey_freq(
    meps_valid,
    table_vars=["JOINT_PAIN_label"],
    stratum_col="STRA9619",
    cluster_col="PSU9619",
    weight_col="PERWTF",
)
print(results_freq.to_string())

print("\n=== Mean expenditures by joint pain status ===")
results_exp = survey_mean(
    meps_valid,
    var_cols=["TOTEXP", "TOTSLF"],
    stratum_col="STRA9619",
    cluster_col="PSU9619",
    weight_col="PERWTF",
    domain_col="JOINT_PAIN_label",
)
print(results_exp.to_string())

spark.stop()
