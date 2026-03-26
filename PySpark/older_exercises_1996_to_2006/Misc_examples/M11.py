"""
M11: Parent Employment Status Merged to Children (2002 Data)

This example demonstrates how to merge parent characteristics to child
records. It identifies parents and children within each family, maps
parent employment status to children, and creates analytic variables
for child-level analysis of parental employment.

Migrated from: SAS/older_exercises_1996_to_2006/Misc_examples/M11/M11.sas
Input files:
  h62.sas7bdat (2002 Full-Year Consolidated Data File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_freq

spark = get_spark()

# ---------------------------------------------------------------------------
# Load 2002 Full-Year Consolidated Data File (HC-062)
# ---------------------------------------------------------------------------
h62 = load_sas7bdat(spark, "C:/MEPS/h62.sas7bdat")

# ---------------------------------------------------------------------------
# Identify children (age < 18) and parents (age 18+, same family)
# ---------------------------------------------------------------------------
meps = (
    h62
    .withColumn("AGE",
        F.when(F.col("AGE02X") >= 0, F.col("AGE02X"))
         .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
         .when(F.col("AGE31X") >= 0, F.col("AGE31X"))
         .otherwise(-1)
    )
    # Create family identifier
    .withColumn("FAMID",
        F.concat(
            F.lpad(F.col("DUID").cast("string"), 7, "0"),
            F.trim(F.col("FAMIDYR").cast("string"))
        )
    )
)

# Children (age < 18) with positive weight
children = (
    meps
    .filter((F.col("AGE") >= 0) & (F.col("AGE") < 18) & (F.col("PERWT02F") > 0))
    .select("DUPERSID", "FAMID", "AGE", "SEX", "PERWT02F", "VARSTR", "VARPSU",
            "DUID", "PID", "CPSFAMID", "FAMIDYR", "FAMSZEYR")
)

# Adults (potential parents) - persons age 18+ in same family
adults = (
    meps
    .filter((F.col("AGE") >= 18) & (F.col("PERWT02F") > 0))
    .withColumn("EMPST_LABEL",
        F.when(F.col("EMPST42").isin(1, 2), "Employed")
         .when(F.col("EMPST42") == 3, "Job to return to")
         .when(F.col("EMPST42") == 4, "Unemployed")
         .otherwise("Other/Unknown")
    )
    .select("DUPERSID", "FAMID", "AGE", "SEX", "EMPST42", "EMPST_LABEL")
    .withColumnRenamed("DUPERSID", "PARENT_ID")
    .withColumnRenamed("AGE", "PARENT_AGE")
    .withColumnRenamed("SEX", "PARENT_SEX")
)

# ---------------------------------------------------------------------------
# Match children with parents in the same family
# Use CPSFAMID to identify parent-child relationships
# ---------------------------------------------------------------------------

# Identify parents as adults in the same DUID/CPSFAMID
parent_info = (
    meps
    .filter((F.col("AGE") >= 18) & (F.col("PERWT02F") > 0))
    .withColumn("PARENT_EMPLOYED",
        F.when(F.col("EMPST42").isin(1, 2), 1).otherwise(0)
    )
    .select("DUID", "CPSFAMID", "DUPERSID", "SEX", "PARENT_EMPLOYED")
)

# Get mothers (female adults) and fathers (male adults)
mothers = (
    parent_info
    .filter(F.col("SEX") == 2)
    .withColumnRenamed("DUPERSID", "MOTHER_ID")
    .withColumnRenamed("PARENT_EMPLOYED", "MOTHER_EMPLOYED")
    .select("DUID", "CPSFAMID", "MOTHER_ID", "MOTHER_EMPLOYED")
)

fathers = (
    parent_info
    .filter(F.col("SEX") == 1)
    .withColumnRenamed("DUPERSID", "FATHER_ID")
    .withColumnRenamed("PARENT_EMPLOYED", "FATHER_EMPLOYED")
    .select("DUID", "CPSFAMID", "FATHER_ID", "FATHER_EMPLOYED")
)

# Keep first mother/father per family (in case of multiple)
mom_window = Window.partitionBy("DUID", "CPSFAMID").orderBy("MOTHER_ID")
mothers = (
    mothers
    .withColumn("rn", F.row_number().over(mom_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

dad_window = Window.partitionBy("DUID", "CPSFAMID").orderBy("FATHER_ID")
fathers = (
    fathers
    .withColumn("rn", F.row_number().over(dad_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# ---------------------------------------------------------------------------
# Merge parent employment status to children
# ---------------------------------------------------------------------------
child_parent = (
    children
    .join(mothers, on=["DUID", "CPSFAMID"], how="left")
    .join(fathers, on=["DUID", "CPSFAMID"], how="left")
    .fillna({"MOTHER_EMPLOYED": -1, "FATHER_EMPLOYED": -1})
)

# Create parental employment category
child_parent = child_parent.withColumn("PARENT_EMP_STATUS",
    F.when(
        (F.col("MOTHER_EMPLOYED") == 1) & (F.col("FATHER_EMPLOYED") == 1),
        "Both Parents Employed"
    )
    .when(
        (F.col("MOTHER_EMPLOYED") == 1) | (F.col("FATHER_EMPLOYED") == 1),
        "One Parent Employed"
    )
    .when(
        (F.col("MOTHER_EMPLOYED") == 0) | (F.col("FATHER_EMPLOYED") == 0),
        "No Parent Employed"
    )
    .otherwise("Unknown/Missing Parent")
)

# ---------------------------------------------------------------------------
# QC: Distribution of parental employment status
# ---------------------------------------------------------------------------
print("=== Children by Parental Employment Status ===")
child_parent.groupBy("PARENT_EMP_STATUS").agg(
    F.count("*").alias("Unweighted_N"),
    F.sum("PERWT02F").alias("Weighted_N")
).orderBy("PARENT_EMP_STATUS").show(truncate=False)

# ---------------------------------------------------------------------------
# Survey-weighted estimates
# ---------------------------------------------------------------------------
print("\n=== Parental Employment Status (Weighted) ===")
results = survey_freq(
    child_parent.filter(F.col("PARENT_EMP_STATUS") != "Unknown/Missing Parent"),
    table_vars=["PARENT_EMP_STATUS"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT02F"
)
print(results)

spark.stop()
