"""
Exercise 4b: Pool MEPS Longitudinal Data Files from Different Panels (17, 18, 19)

Illustrates how to pool longitudinal data from different panels.
Example: Population age 26-30, uninsured with high income in first year.
Estimates insurance status in second year.

Input files:
  - C:/MEPS/h183.sas7bdat (Panel 19 Longitudinal)
  - C:/MEPS/h172.sas7bdat (Panel 18 Longitudinal)
  - C:/MEPS/h164.sas7bdat (Panel 17 Longitudinal)

Replaces: SAS/workshop_exercises/exercise_4b/Exercise4b.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_freq

spark = get_spark()

# Load and combine panels
keep_cols = ["DUPERSID", "INSCOVY1", "INSCOVY2", "LONGWT", "VARSTR",
             "VARPSU", "POVCATY1", "AGEY1X", "PANEL"]

h164 = load_sas7bdat(spark, "C:/MEPS/h164.sas7bdat").select(keep_cols)
h172 = load_sas7bdat(spark, "C:/MEPS/h172.sas7bdat").select(keep_cols)
h183 = load_sas7bdat(spark, "C:/MEPS/h183.sas7bdat").select(keep_cols)

pool = (
    h164.unionByName(h172).unionByName(h183)
    .withColumn("POOLWT", F.col("LONGWT") / 3)
    # Create subpopulation: uninsured (INSCOVY1=3), age 26-30, high income (POVCATY1=5)
    .withColumn("SUBPOP",
        F.when(
            (F.col("INSCOVY1") == 3) &
            (F.col("AGEY1X") >= 26) & (F.col("AGEY1X") <= 30) &
            (F.col("POVCATY1") == 5), 1
        ).otherwise(2))
    # Label insurance status in year 2
    .withColumn("INSCOVY2_label",
        F.when(F.col("INSCOVY2") == -1, "-1 INAPPLICABLE")
        .when(F.col("INSCOVY2") == 1, "1 ANY PRIVATE")
        .when(F.col("INSCOVY2") == 2, "2 PUBLIC ONLY")
        .when(F.col("INSCOVY2") == 3, "3 UNINSURED"))
)

# QC
print("=== Supporting crosstab ===")
pool.groupBy("SUBPOP").count().show()
pool.groupBy("SUBPOP", "PANEL").count().orderBy("SUBPOP", "PANEL").show()

# Insurance status in second year for subpopulation
print("=== Insurance status in Y2 for age 26-30, uninsured, high income in Y1 ===")
pool_sub = pool.filter(F.col("SUBPOP") == 1)
results = survey_freq(
    pool_sub,
    table_vars=["INSCOVY2_label"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="POOLWT",
)
print(results.to_string())

spark.stop()
