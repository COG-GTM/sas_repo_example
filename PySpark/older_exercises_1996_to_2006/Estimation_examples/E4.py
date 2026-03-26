"""
E4: Family-Level Aggregation (2001 Data)

This example shows how to aggregate person-level data to the family level
using PySpark Window functions (equivalent to SAS FIRST./LAST. BY-group
processing) and compute family-level estimates.

Migrated from: SAS/older_exercises_1996_to_2006/Estimation_examples/E4/E4.sas
Input file: h60.sas7bdat (2001 Full-Year Population Characteristics File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# ---------------------------------------------------------------------------
# Load 2001 Full-Year Consolidated Data File (HC-060)
# ---------------------------------------------------------------------------
h60 = load_sas7bdat(spark, "C:/MEPS/h60.sas7bdat")

# ---------------------------------------------------------------------------
# Create family identifier: DUID (5-digit zero-padded) + FAMIDYR
# ---------------------------------------------------------------------------
meps = (
    h60
    .withColumn("DUIDFAMY",
        F.concat(
            F.lpad(F.col("DUID").cast("string"), 5, "0"),
            F.trim(F.col("FAMIDYR").cast("string"))
        )
    )
)

# ---------------------------------------------------------------------------
# Aggregate to family level using Window functions
# ---------------------------------------------------------------------------
fam_window = Window.partitionBy("DUIDFAMY")

meps = (
    meps
    # Sum expenditures to family level
    .withColumn("FAMTOT01", F.sum("TOTEXP01").over(fam_window))
    # Family weight (from first family member)
    .withColumn("FAMWT01F", F.first("FAMWT01F").over(fam_window))
    # Family size
    .withColumn("FAMSZEYR_count", F.count("*").over(fam_window))
    # Survey design variables (same for all family members)
    .withColumn("FAM_VARSTR", F.first("VARSTR").over(fam_window))
    .withColumn("FAM_VARPSU", F.first("VARPSU").over(fam_window))
)

# Deduplicate to family level (keep first record per family)
row_window = Window.partitionBy("DUIDFAMY").orderBy("DUPERSID")
fam_level = (
    meps
    .withColumn("row_num", F.row_number().over(row_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    # Filter to families with positive weight
    .filter(F.col("FAMWT01F") > 0)
)

# Create family size category
fam_level = fam_level.withColumn("FAMSZEYR_label",
    F.when(F.col("FAMSZEYR") == 1, "1 person")
     .when(F.col("FAMSZEYR") == 2, "2 persons")
     .when(F.col("FAMSZEYR") == 3, "3 persons")
     .when(F.col("FAMSZEYR") >= 4, "4+ persons")
     .otherwise("Unknown")
)

# ---------------------------------------------------------------------------
# QC: Family size distribution
# ---------------------------------------------------------------------------
print("=== Family Size Distribution ===")
fam_level.groupBy("FAMSZEYR_label").count().orderBy("FAMSZEYR_label").show()

# ---------------------------------------------------------------------------
# Estimates: Mean family expenditures overall
# ---------------------------------------------------------------------------
print("=== Overall Mean Family Expenditures ===")
results_overall = survey_mean(
    fam_level,
    var_cols=["FAMTOT01"],
    stratum_col="FAM_VARSTR",
    cluster_col="FAM_VARPSU",
    weight_col="FAMWT01F"
)
print(results_overall)

# ---------------------------------------------------------------------------
# Estimates: Mean family expenditures by family size
# ---------------------------------------------------------------------------
print("\n=== Mean Family Expenditures by Family Size ===")
results_size = survey_mean(
    fam_level,
    var_cols=["FAMTOT01"],
    stratum_col="FAM_VARSTR",
    cluster_col="FAM_VARPSU",
    weight_col="FAMWT01F",
    domain_col="FAMSZEYR_label"
)
print(results_size)

spark.stop()
