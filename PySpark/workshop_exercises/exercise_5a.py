"""
Exercise 5a: Constructing Family-level Variables from Person-level Data, 2015

Illustrates:
  - Constructing family-level variables from person-level data
  - Using Window functions as PySpark equivalent of FIRST./LAST. BY-group processing
  - Deduplicating to family-level observations

Input file: C:/MEPS/h181.sas7bdat (2015 Full-Year File)

Replaces: SAS/workshop_exercises/exercise_5a/Exercise5a.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F, Window
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# Load 2015 FYC
h181 = load_sas7bdat(spark, "C:/MEPS/h181.sas7bdat")

# Select needed variables
persons = h181.select(
    "DUPERSID", "DUID", "CPSFAMID", "AGELAST",
    "TOTSLF15", "TTLP15X",
    "VARSTR", "VARPSU", "PERWT15F",
)

# Define window for family-level aggregation
fam_window = Window.partitionBy("DUID", "CPSFAMID")

# Create family-level variables using Window functions
# (equivalent to SAS FIRST./LAST. BY-group processing)
fam_data = (
    persons
    .withColumn("FAMSIZE", F.count("*").over(fam_window))
    .withColumn("FAMOOP", F.sum("TOTSLF15").over(fam_window))
    .withColumn("FAMINC", F.sum("TTLP15X").over(fam_window))
    # Keep one record per family (equivalent to NODUPKEY on DUID CPSFAMID)
    # Use row_number to pick first person per family deterministically
    .withColumn("_rn", F.row_number().over(
        fam_window.orderBy("DUPERSID")))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# QC
print("=== Family-level data (first 10 rows) ===")
fam_data.select("DUID", "CPSFAMID", "FAMSIZE", "FAMOOP", "FAMINC").show(10)

print("=== Distribution of family size ===")
fam_data.groupBy("FAMSIZE").count().orderBy("FAMSIZE").show()

# Estimates at the family level
print("=== Family-level survey estimates, 2015 ===")
results = survey_mean(
    fam_data,
    var_cols=["FAMSIZE", "FAMOOP", "FAMINC"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
)
print(results.to_string())

results_total = survey_total(
    fam_data,
    var_cols=["FAMSIZE", "FAMOOP", "FAMINC"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
)
print(results_total.to_string())

spark.stop()
