"""
Health Insurance, 2016:
 - Number/percent of people by insurance coverage and age groups

Input file: C:/MEPS/h192.ssp (2016 full-year consolidated)

Replaces: SAS/summary_tables_examples/ins_age_2016.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_ssp
from utils.survey_utils import survey_freq
from utils.format_mappings import INSURANCE_FORMAT

# Initialize Spark
spark = get_spark()

# Load FYC file
h192 = load_ssp(spark, "C:/MEPS/h192.ssp")

# Define variables
meps = (
    h192
    # Create age groups
    .withColumn("age_grp",
        F.when(F.col("AGELAST") < 18, "Under 18")
        .when((F.col("AGELAST") >= 18) & (F.col("AGELAST") <= 64), "18-64")
        .when(F.col("AGELAST") >= 65, "65+"))

    # Insurance coverage label for persons under 65
    .withColumn("ins_label",
        F.when(F.col("INSURC16") == 1, "<65, Any private")
        .when(F.col("INSURC16") == 2, "<65, Public only")
        .when(F.col("INSURC16") == 3, "<65, Uninsured")
        .when(F.col("INSURC16") == 4, "65+, Medicare only")
        .when(F.col("INSURC16") == 5, "65+, Medicare and private")
        .when(F.col("INSURC16") == 6, "65+, Medicare and other public")
        .when(F.col("INSURC16").isin(7, 8), "65+, No medicare")
        .otherwise("Missing"))
)

# Calculate estimates using survey procedures
print("=== Survey-weighted frequencies: Insurance coverage by age group ===")
results = survey_freq(
    meps,
    table_vars=["ins_label", "age_grp"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
)
print(results.to_string())

# Summary crosstab
print("\n=== Crosstab: Insurance by age group ===")
(meps
    .groupBy("ins_label", "age_grp")
    .count()
    .orderBy("ins_label", "age_grp")
    .show(50))

spark.stop()
