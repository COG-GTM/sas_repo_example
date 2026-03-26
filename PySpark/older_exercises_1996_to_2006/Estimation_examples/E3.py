"""
E3: Longitudinal Analysis with Panel-Based Tracking (1999-2000)

This example shows how to use MEPS longitudinal data to track changes
in insurance status and expenditures from one year to the next
for persons in Panel 4.

Migrated from: SAS/older_exercises_1996_to_2006/Estimation_examples/E3/E3.sas
Input files:
  h38.sas7bdat (1999 FYC), h50.sas7bdat (2000 FYC),
  h58.sas7bdat (Panel 4 Longitudinal)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_mean

spark = get_spark()

# ---------------------------------------------------------------------------
# Load data files
# ---------------------------------------------------------------------------
h38 = load_sas7bdat(spark, "C:/MEPS/h38.sas7bdat")   # 1999 FYC
h50 = load_sas7bdat(spark, "C:/MEPS/h50.sas7bdat")   # 2000 FYC
h58 = load_sas7bdat(spark, "C:/MEPS/h58.sas7bdat")   # Panel 4 Longitudinal

# ---------------------------------------------------------------------------
# Prepare 1999 data (Panel 4 persons only)
# ---------------------------------------------------------------------------
fyc99 = (
    h38
    .filter(F.col("PANEL99") == 4)
    .withColumn("INSCOV99",
        F.when(F.col("INSCOV99") == 1, 1)   # Any private
         .when(F.col("INSCOV99") == 2, 2)   # Public only
         .when(F.col("INSCOV99") == 3, 3)   # Uninsured
         .otherwise(F.col("INSCOV99"))
    )
    .withColumn("ANY_EXP99",
        F.when(F.col("TOTEXP99") > 0, 100).otherwise(0)
    )
    .select("DUPERSID", "INSCOV99", "TOTEXP99", "ANY_EXP99")
)

# ---------------------------------------------------------------------------
# Prepare 2000 data (Panel 4 persons only)
# ---------------------------------------------------------------------------
fyc00 = (
    h50
    .filter(F.col("PANEL00") == 4)
    .withColumn("INSCOV00",
        F.when(F.col("INSCOV00") == 1, 1)
         .when(F.col("INSCOV00") == 2, 2)
         .when(F.col("INSCOV00") == 3, 3)
         .otherwise(F.col("INSCOV00"))
    )
    .withColumn("ANY_EXP00",
        F.when(F.col("TOTEXP00") > 0, 100).otherwise(0)
    )
    .select("DUPERSID", "INSCOV00", "TOTEXP00", "ANY_EXP00")
)

# ---------------------------------------------------------------------------
# Prepare longitudinal file (Panel 4, in both years)
# ---------------------------------------------------------------------------
longit = (
    h58
    .filter(F.col("YRINDP4") == 1)  # Persons in both years
    .select("DUPERSID", "LONGWT4F", "VARSTR", "VARPSU")
)

# ---------------------------------------------------------------------------
# Merge all three files
# ---------------------------------------------------------------------------
merged = (
    longit
    .join(fyc99, on="DUPERSID", how="inner")
    .join(fyc00, on="DUPERSID", how="inner")
)

# Create insurance transition variable
merged = merged.withColumn("INS_TRANS",
    F.concat(
        F.when(F.col("INSCOV99") == 1, F.lit("Private"))
         .when(F.col("INSCOV99") == 2, F.lit("Public"))
         .when(F.col("INSCOV99") == 3, F.lit("Uninsured"))
         .otherwise(F.lit("Other")),
        F.lit(" -> "),
        F.when(F.col("INSCOV00") == 1, F.lit("Private"))
         .when(F.col("INSCOV00") == 2, F.lit("Public"))
         .when(F.col("INSCOV00") == 3, F.lit("Uninsured"))
         .otherwise(F.lit("Other"))
    )
)

# Create insurance status labels
merged = merged.withColumn("INSCOV99_label",
    F.when(F.col("INSCOV99") == 1, "1 Any private")
     .when(F.col("INSCOV99") == 2, "2 Public only")
     .when(F.col("INSCOV99") == 3, "3 Uninsured")
     .otherwise("Other")
)

# ---------------------------------------------------------------------------
# QC: Insurance transition frequencies
# ---------------------------------------------------------------------------
print("=== Insurance Transition Frequencies ===")
merged.groupBy("INS_TRANS").count().orderBy("INS_TRANS").show(truncate=False)

# ---------------------------------------------------------------------------
# Estimates: Mean expenditures and percent with expense by 1999 insurance
# ---------------------------------------------------------------------------
print("=== Mean Expenditures (Y1=1999, Y2=2000) by 1999 Insurance Status ===")
results = survey_mean(
    merged,
    var_cols=["TOTEXP99", "TOTEXP00", "ANY_EXP99", "ANY_EXP00"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="LONGWT4F",
    domain_col="INSCOV99_label"
)
print(results)

spark.stop()
