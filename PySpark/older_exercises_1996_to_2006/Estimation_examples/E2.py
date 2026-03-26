"""
E2: Pooling Data Across Years with CPI Adjustment (1996-1999)

This example shows how to pool MEPS data across years, adjust expenditures
for inflation using the CPI-U, and compute estimates using the Pooled
Estimation Variance file for proper standard errors.

Migrated from: SAS/older_exercises_1996_to_2006/Estimation_examples/E2/E2.sas
Input files:
  h12.ssp (1996 FYC), h20.ssp (1997 FYC),
  h28.ssp (1998 FYC), h38.ssp (1999 FYC),
  h36.ssp (1996-2002 Pooled Estimation Variance File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_ssp, get_spark
from utils.survey_utils import survey_mean

spark = get_spark()

# ---------------------------------------------------------------------------
# CPI-U Adjustment Factors (standardize to 1999 dollars)
# CPI-U Medical: 1996=156.9, 1997=162.0, 1998=167.3, 1999=166.6 (estimate)
# Using CPI-U All Items: 1996=156.9, 1997=160.5, 1998=163.0, 1999=166.6
# ---------------------------------------------------------------------------
CPI_1999 = 166.6
CPI_FACTORS = {
    1996: CPI_1999 / 156.9,
    1997: CPI_1999 / 160.5,
    1998: CPI_1999 / 163.0,
    1999: 1.0,
}

# ---------------------------------------------------------------------------
# Load data files
# ---------------------------------------------------------------------------
h12 = load_ssp(spark, "C:/MEPS/h12.ssp")   # 1996 FYC
h20 = load_ssp(spark, "C:/MEPS/h20.ssp")   # 1997 FYC
h28 = load_ssp(spark, "C:/MEPS/h28.ssp")   # 1998 FYC
h38 = load_ssp(spark, "C:/MEPS/h38.ssp")   # 1999 FYC
h36 = load_ssp(spark, "C:/MEPS/h36.ssp")   # Pooled Estimation Variance File

# ---------------------------------------------------------------------------
# Prepare 1996-1997 pooled data (2 years)
# ---------------------------------------------------------------------------
fyc96 = (
    h12
    .withColumn("YEAR", F.lit(1996))
    .withColumn("TOTEXP", F.col("TOTEXP96") * F.lit(CPI_FACTORS[1996]))
    .withColumn("POOLWT", F.col("PERWT96F") / 2)
    .withColumn("AGE",
        F.when(F.col("AGE96X") >= 0, F.col("AGE96X"))
         .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
         .when(F.col("AGE31X") >= 0, F.col("AGE31X"))
         .otherwise(-1)
    )
    .select("DUPERSID", "YEAR", "TOTEXP", "POOLWT", "AGE", "PANEL")
)

fyc97 = (
    h20
    .withColumn("YEAR", F.lit(1997))
    .withColumn("TOTEXP", F.col("TOTEXP97") * F.lit(CPI_FACTORS[1997]))
    .withColumn("POOLWT", F.col("PERWT97F") / 2)
    .withColumn("AGE",
        F.when(F.col("AGE97X") >= 0, F.col("AGE97X"))
         .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
         .when(F.col("AGE31X") >= 0, F.col("AGE31X"))
         .otherwise(-1)
    )
    .select("DUPERSID", "YEAR", "TOTEXP", "POOLWT", "AGE", "PANEL")
)

pooled_96_97 = fyc96.unionByName(fyc97)

# ---------------------------------------------------------------------------
# Prepare 1998-1999 pooled data (2 years)
# ---------------------------------------------------------------------------
fyc98 = (
    h28
    .withColumn("YEAR", F.lit(1998))
    .withColumn("TOTEXP", F.col("TOTEXP98") * F.lit(CPI_FACTORS[1998]))
    .withColumn("POOLWT", F.col("PERWT98F") / 2)
    .withColumn("AGE",
        F.when(F.col("AGE98X") >= 0, F.col("AGE98X"))
         .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
         .when(F.col("AGE31X") >= 0, F.col("AGE31X"))
         .otherwise(-1)
    )
    .select("DUPERSID", "YEAR", "TOTEXP", "POOLWT", "AGE", "PANEL")
)

fyc99 = (
    h38
    .withColumn("YEAR", F.lit(1999))
    .withColumn("TOTEXP", F.col("TOTEXP99"))  # No adjustment needed
    .withColumn("POOLWT", F.col("PERWT99F") / 2)
    .withColumn("AGE",
        F.when(F.col("AGE99X") >= 0, F.col("AGE99X"))
         .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
         .when(F.col("AGE31X") >= 0, F.col("AGE31X"))
         .otherwise(-1)
    )
    .select("DUPERSID", "YEAR", "TOTEXP", "POOLWT", "AGE", "PANEL")
)

pooled_98_99 = fyc98.unionByName(fyc99)

# ---------------------------------------------------------------------------
# Merge with Pooled Estimation Variance File for proper SEs
# ---------------------------------------------------------------------------
pooled_var = h36.select("DUPERSID", "PANEL", "STRA9602", "PSU9602")

pooled_96_97 = pooled_96_97.join(pooled_var, on=["DUPERSID", "PANEL"], how="left")

# Create age category
pooled_96_97 = pooled_96_97.withColumn("AGE_label",
    F.when((F.col("AGE") >= 0) & (F.col("AGE") <= 17), "0-17")
     .when((F.col("AGE") >= 18) & (F.col("AGE") <= 44), "18-44")
     .when((F.col("AGE") >= 45) & (F.col("AGE") <= 64), "45-64")
     .when(F.col("AGE") >= 65, "65+")
     .otherwise("Unknown")
)

pooled_98_99 = pooled_98_99.join(pooled_var, on=["DUPERSID", "PANEL"], how="left")

pooled_98_99 = pooled_98_99.withColumn("AGE_label",
    F.when((F.col("AGE") >= 0) & (F.col("AGE") <= 17), "0-17")
     .when((F.col("AGE") >= 18) & (F.col("AGE") <= 44), "18-44")
     .when((F.col("AGE") >= 45) & (F.col("AGE") <= 64), "45-64")
     .when(F.col("AGE") >= 65, "65+")
     .otherwise("Unknown")
)

# ---------------------------------------------------------------------------
# Estimates: 1996-1997 pooled, by age group (in 1999 dollars)
# ---------------------------------------------------------------------------
print("=== 1996-1997 Pooled Mean Expenditures by Age (1999$) ===")
results_96_97 = survey_mean(
    pooled_96_97,
    var_cols=["TOTEXP"],
    stratum_col="STRA9602",
    cluster_col="PSU9602",
    weight_col="POOLWT",
    domain_col="AGE_label"
)
print(results_96_97)

# ---------------------------------------------------------------------------
# Estimates: 1998-1999 pooled, by age group (in 1999 dollars)
# ---------------------------------------------------------------------------
print("\n=== 1998-1999 Pooled Mean Expenditures by Age (1999$) ===")
results_98_99 = survey_mean(
    pooled_98_99,
    var_cols=["TOTEXP"],
    stratum_col="STRA9602",
    cluster_col="PSU9602",
    weight_col="POOLWT",
    domain_col="AGE_label"
)
print(results_98_99)

spark.stop()
