"""
M9: Insurance Status and Healthcare Utilization (2003 Data)

This example demonstrates how to use insurance status variables to
analyze healthcare utilization patterns among different insurance groups.

Migrated from: SAS/older_exercises_1996_to_2006/Misc_examples/M9/M9.sas
Input file: h79.sas7bdat (2003 Full-Year Consolidated Data File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_mean

spark = get_spark()

# ---------------------------------------------------------------------------
# Load 2003 Full-Year Consolidated Data File (HC-079)
# ---------------------------------------------------------------------------
h79 = load_sas7bdat(spark, "C:/MEPS/h79.sas7bdat")

# ---------------------------------------------------------------------------
# Create analytic variables
# ---------------------------------------------------------------------------
meps = (
    h79
    .filter(F.col("PERWT03F") > 0)
    # Insurance coverage categories
    .withColumn("INSCOV_label",
        F.when(F.col("INSCOV03") == 1, "1 Any Private")
         .when(F.col("INSCOV03") == 2, "2 Public Only")
         .when(F.col("INSCOV03") == 3, "3 Uninsured")
         .otherwise("Unknown")
    )
    # Has any expense
    .withColumn("ANY_EXP",
        F.when(F.col("TOTEXP03") > 0, 1).otherwise(0)
    )
    # Has office-based visit
    .withColumn("ANY_OBV",
        F.when(F.col("OBTOTV03") > 0, 1).otherwise(0)
    )
    # Has ER visit
    .withColumn("ANY_ER",
        F.when(F.col("ERTOT03") > 0, 1).otherwise(0)
    )
    # Has inpatient stay
    .withColumn("ANY_IP",
        F.when(F.col("IPDIS03") > 0, 1).otherwise(0)
    )
    # Has PMED fill
    .withColumn("ANY_PMED",
        F.when(F.col("RXTOT03") > 0, 1).otherwise(0)
    )
)

# ---------------------------------------------------------------------------
# Survey-weighted estimates: Utilization by insurance status
# ---------------------------------------------------------------------------
print("=== Percent with Any Expense by Insurance Status ===")
results_util = survey_mean(
    meps,
    var_cols=["ANY_EXP", "ANY_OBV", "ANY_ER", "ANY_IP", "ANY_PMED"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F",
    domain_col="INSCOV_label"
)
print(results_util)

# ---------------------------------------------------------------------------
# Mean total expenditures by insurance status
# ---------------------------------------------------------------------------
print("\n=== Mean Total Expenditures by Insurance Status ===")
results_exp = survey_mean(
    meps,
    var_cols=["TOTEXP03"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT03F",
    domain_col="INSCOV_label"
)
print(results_exp)

spark.stop()
