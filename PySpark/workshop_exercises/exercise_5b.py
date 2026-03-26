"""
Exercise 5b: Constructing Insurance Status Variables, 2015

Illustrates:
  - Constructing insurance status variables from monthly insurance data
  - Using array-based processing (SAS ARRAY equivalent)
  - Creating full-year insurance coverage flags

Input file: C:/MEPS/h181.sas7bdat (2015 Full-Year File)

Replaces: SAS/workshop_exercises/exercise_5b/Exercise5b.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_freq

spark = get_spark()

# Load 2015 FYC
h181 = load_sas7bdat(spark, "C:/MEPS/h181.sas7bdat")

# Monthly insurance variables (January-December)
# PRIDK: Private insurance, group or non-group
# HPDK: Private insurance through HMO/managed care
# MCDK: Medicare
# MCDK: Medicaid/SCHIP
# UNINS: Uninsured

# Define month suffixes for the 12 rounds/months
months_suffix = ["JA", "FE", "MA", "AP", "MY", "JU", "JL", "AU", "SE", "OC", "NO", "DE"]

# Count months with each type of coverage
# Private insurance: PRID** = 1 for any month
priv_cols = [f"PRID{m}15" for m in months_suffix]
# Medicare: MCD** = 1 for any month
mcare_cols = [f"MCRD{m}15" for m in months_suffix]
# Medicaid: MCDK** = 1 for any month
mcaid_cols = [f"MCDD{m}15" for m in months_suffix]
# Uninsured: UNINS** = 1 for any month (derived)

meps = h181

# Count months with private insurance
priv_count_expr = sum([F.when(F.col(c) == 1, 1).otherwise(0) for c in priv_cols])
meps = meps.withColumn("N_PRIV_MONTHS", priv_count_expr)

# Count months with Medicare
mcare_count_expr = sum([F.when(F.col(c) == 1, 1).otherwise(0) for c in mcare_cols])
meps = meps.withColumn("N_MCARE_MONTHS", mcare_count_expr)

# Count months with Medicaid
mcaid_count_expr = sum([F.when(F.col(c) == 1, 1).otherwise(0) for c in mcaid_cols])
meps = meps.withColumn("N_MCAID_MONTHS", mcaid_count_expr)

# Create full-year insurance flags
meps = (
    meps
    # Full-year private
    .withColumn("FULL_YEAR_PRIV",
        F.when(F.col("N_PRIV_MONTHS") == 12, 1).otherwise(0))
    # Any private during year
    .withColumn("ANY_PRIV",
        F.when(F.col("N_PRIV_MONTHS") > 0, 1).otherwise(0))
    # Full-year Medicare
    .withColumn("FULL_YEAR_MCARE",
        F.when(F.col("N_MCARE_MONTHS") == 12, 1).otherwise(0))
    # Full-year Medicaid
    .withColumn("FULL_YEAR_MCAID",
        F.when(F.col("N_MCAID_MONTHS") == 12, 1).otherwise(0))
    # Race/ethnicity label
    .withColumn("RACETHX_label",
        F.when(F.col("RACETHX") == 1, "Hispanic")
        .when(F.col("RACETHX") == 2, "NH White")
        .when(F.col("RACETHX") == 3, "NH Black")
        .when(F.col("RACETHX") == 4, "NH Asian")
        .when(F.col("RACETHX") == 5, "NH Other/Multiple"))
)

# QC
print("=== Distribution of insurance months ===")
meps.select("N_PRIV_MONTHS", "N_MCARE_MONTHS", "N_MCAID_MONTHS").describe().show()

# Estimates by race/ethnicity
print("=== Full-year insurance coverage by race/ethnicity, 2015 ===")
results = survey_mean(
    meps,
    var_cols=["FULL_YEAR_PRIV", "ANY_PRIV", "FULL_YEAR_MCARE", "FULL_YEAR_MCAID"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
    domain_col="RACETHX_label",
)
print(results.to_string())

spark.stop()
