"""
Exercise 4a: Pool MEPS Data Files from Different Years (2015 and 2016)

Illustrates how to pool MEPS data files from different years.
Example: Population age 26-30 who are uninsured but have high income.

Input files:
  - C:/MEPS/h192.sas7bdat (2016 Full-Year File)
  - C:/MEPS/h181.sas7bdat (2015 Full-Year File)

Replaces: SAS/workshop_exercises/exercise_4a/Exercise4a.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean

spark = get_spark()

# Load 2015 data
h181 = load_sas7bdat(spark, "C:/MEPS/h181.sas7bdat")
yr1 = (
    h181
    .select("DUPERSID", "INSCOV15", "PERWT15F", "VARSTR", "VARPSU",
            "POVCAT15", "AGELAST", "TOTSLF15")
    .filter(F.col("PERWT15F") > 0)
    .withColumnRenamed("INSCOV15", "INSCOV")
    .withColumnRenamed("PERWT15F", "PERWT")
    .withColumnRenamed("POVCAT15", "POVCAT")
    .withColumnRenamed("TOTSLF15", "TOTSLF")
)

# Load 2016 data
h192 = load_sas7bdat(spark, "C:/MEPS/h192.sas7bdat")
yr2 = (
    h192
    .select("DUPERSID", "INSCOV16", "PERWT16F", "VARSTR", "VARPSU",
            "POVCAT16", "AGELAST", "TOTSLF16")
    .filter(F.col("PERWT16F") > 0)
    .withColumnRenamed("INSCOV16", "INSCOV")
    .withColumnRenamed("PERWT16F", "PERWT")
    .withColumnRenamed("POVCAT16", "POVCAT")
    .withColumnRenamed("TOTSLF16", "TOTSLF")
)

# Pool data and create pooled weight (divide by number of years)
pool = (
    yr1.unionByName(yr2, allowMissingColumns=True)
    .withColumn("POOLWT", F.col("PERWT") / 2)
    # Create subpopulation: age 26-30, high income (POVCAT=5), uninsured (INSCOV=3)
    .withColumn("SUBPOP",
        F.when(
            (F.col("AGELAST") >= 26) & (F.col("AGELAST") <= 30) &
            (F.col("POVCAT") == 5) & (F.col("INSCOV") == 3), 1
        ).otherwise(2))
)

# QC: Check missing values
print("=== Check missing values on combined data ===")
pool.select([F.count(F.when(F.col(c).isNull(), c)).alias(c)
             for c in pool.columns]).show()

# QC: Supporting crosstab
print("=== Supporting crosstab for SUBPOP ===")
pool.groupBy("SUBPOP").count().show()

# Weighted estimate on TOTSLF for combined data
print("=== Weighted estimate: age 26-30, uninsured, high income ===")
results = survey_mean(
    pool,
    var_cols=["TOTSLF"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="POOLWT",
    domain_col="SUBPOP",
)
print(results.to_string())

spark.stop()
