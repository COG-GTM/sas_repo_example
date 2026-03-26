"""
EM1: Perceived Health Status and Weekly Earnings (2002 Data)

This example shows how to build an analytic file and create new variables
to examine the relationship between perceived health status and weekly
earnings. Person-level records are divided into quartiles based on weekly
earnings and person-level weights, resulting in 4 equally weighted quartiles.

Migrated from: SAS/older_exercises_1996_to_2006/Employment_examples/EM1/EM1.sas
Input file: h62.sas7bdat (2002 Full-Year Population Characteristics File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_mean

spark = get_spark()

# ---------------------------------------------------------------------------
# Load 2002 Full-Year Consolidated Data File (HC-062)
# ---------------------------------------------------------------------------
h62 = load_sas7bdat(spark, "C:/MEPS/h62.sas7bdat")

# ---------------------------------------------------------------------------
# Subset to persons with current main job (CMJ) in rounds 4/2 who
# reported earnings, weekly hours, and perceived health status
# ---------------------------------------------------------------------------
h62a = (
    h62
    .filter(
        (F.col("EMPST42").isin(1, 2)) &
        ((F.col("SELFCM42") == 2) |
         ((F.col("SELFCM42") == -2) & (F.col("SELFCM31") == 2))) &
        ((F.col("HRWG42X") == -10) | (F.col("HRWG42X") > 0) |
         ((F.col("HRWG42X") == -2) &
          ((F.col("HRWG31X") == -10) | (F.col("HRWG31X") > 0)))) &
        ((F.col("HOUR42") > 0) |
         ((F.col("HOUR42") == -2) & (F.col("HOUR31") > 0))) &
        (F.col("RTHLTH42").isin(1, 2, 3, 4, 5)) &
        (F.col("TEMPJB42") == 2) &
        (F.col("AGE42X") > 24)
    )
    # Construct hourly wage variable with fallback
    .withColumn("HRLYWAGE",
        F.when(F.col("HRWG42X") == -2, F.col("HRWG31X"))
         .otherwise(F.col("HRWG42X"))
    )
    .withColumn("HRLYWAGE",
        F.when(F.col("HRLYWAGE") == -10, 61.98)
         .otherwise(F.col("HRLYWAGE"))
    )
    # Construct hours variable with fallback
    .withColumn("HOURS",
        F.when(F.col("HOUR42") > 0, F.col("HOUR42"))
         .when(F.col("HOUR42") == -2, F.col("HOUR31"))
         .otherwise(F.col("HOUR42"))
    )
    # Weekly earnings
    .withColumn("WKLYEARN", F.col("HRLYWAGE") * F.col("HOURS"))
    .select("DUPERSID", "RTHLTH42", "WKLYEARN", "PERWT02P", "VARSTR", "VARPSU")
)

# ---------------------------------------------------------------------------
# Create weighted quartiles of weekly earnings
# Uses cumulative sum of weights approach
# ---------------------------------------------------------------------------
# Sort by weekly earnings
h62a = h62a.orderBy("WKLYEARN")

# Compute total weight
total_wt = h62a.agg(F.sum("PERWT02P")).collect()[0][0]

# Use Window function to compute cumulative weight
earn_window = Window.orderBy("WKLYEARN")
h62b = (
    h62a
    .withColumn("CUM_WT", F.sum("PERWT02P").over(earn_window))
    .withColumn("QUARTILE",
        F.when(F.col("CUM_WT") <= total_wt * 0.25, 1)
         .when(F.col("CUM_WT") <= total_wt * 0.50, 2)
         .when(F.col("CUM_WT") <= total_wt * 0.75, 3)
         .otherwise(4)
    )
    .withColumn("QUARTILE_label",
        F.when(F.col("QUARTILE") == 1, "1 Lowest")
         .when(F.col("QUARTILE") == 2, "2")
         .when(F.col("QUARTILE") == 3, "3")
         .when(F.col("QUARTILE") == 4, "4 Highest")
         .otherwise("Unknown")
    )
    .drop("CUM_WT")
)

# ---------------------------------------------------------------------------
# QC: Weighted frequency of quartiles
# ---------------------------------------------------------------------------
print("=== Weighted Frequency of Quartiles ===")
h62b.groupBy("QUARTILE_label").agg(
    F.count("*").alias("N"),
    F.sum("PERWT02P").alias("Weighted_N")
).orderBy("QUARTILE_label").show()

print("=== Quartile by Health Status Cross-tabulation ===")
h62b.groupBy("QUARTILE_label", "RTHLTH42").count().orderBy(
    "QUARTILE_label", "RTHLTH42"
).show(50)

# ---------------------------------------------------------------------------
# Estimates: Mean health status by weekly earnings quartile
# (1=Excellent, 2=Very Good, 3=Good, 4=Fair, 5=Poor)
# ---------------------------------------------------------------------------
print("=== Mean Health Status by Weekly Earnings Quartile ===")
results = survey_mean(
    h62b,
    var_cols=["RTHLTH42"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT02P",
    domain_col="QUARTILE_label"
)
print(results)

spark.stop()
