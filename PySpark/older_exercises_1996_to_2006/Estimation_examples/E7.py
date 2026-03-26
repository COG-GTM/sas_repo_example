"""
E7: Colonoscopy Screening Among Adults 50+ (2005 Data)

This example shows how to compute estimates from MEPS Statistical Brief #188,
'Screening Colonoscopy Among U.S. Noninstitutionalized Adult Population
Age 50 and Older, 2005'.

NOTE: The entire 2005 MEPS population must be used in the analysis to generate
accurate standard errors. An age category variable is used in the SURVEYFREQ
TABLE statement to request estimates for the age groups desired.

Migrated from: SAS/older_exercises_1996_to_2006/Estimation_examples/E7/E7.sas
Input file: h97.sas7bdat (2005 Full-Year Consolidated Data File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_freq

spark = get_spark()

# ---------------------------------------------------------------------------
# Load 2005 Full-Year Consolidated Data File (HC-097)
# ---------------------------------------------------------------------------
h97 = load_sas7bdat(spark, "C:/MEPS/h97.sas7bdat")

# ---------------------------------------------------------------------------
# Create analytic variables
# ---------------------------------------------------------------------------
meps = (
    h97
    # BOWELYES: had colonoscopy (BOWEL53=1)
    .withColumn("BOWELYES",
        F.when(F.col("BOWEL53") == 1, 1).otherwise(0)
    )
    # AGE with fallback logic
    .withColumn("AGE",
        F.when(F.col("AGE05X") >= 0, F.col("AGE05X"))
         .when(F.col("AGE53X") >= 0, F.col("AGE53X"))
         .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
         .when(F.col("AGE31X") >= 0, F.col("AGE31X"))
         .otherwise(-1)
    )
    # AGECAT: 3-level age category
    .withColumn("AGECAT",
        F.when((F.col("AGE") >= 0) & (F.col("AGE") <= 49), 1)
         .when((F.col("AGE") >= 50) & (F.col("AGE") <= 64), 2)
         .when(F.col("AGE") >= 65, 3)
         .otherwise(-1)
    )
    .withColumn("AGECAT_label",
        F.when(F.col("AGECAT") == 1, "1 Age 0-49")
         .when(F.col("AGECAT") == 2, "2 Age 50-64")
         .when(F.col("AGECAT") == 3, "3 Age 65+")
         .otherwise("Unknown")
    )
    # AGE50PLUS: 2-level age category
    .withColumn("AGE50PLUS",
        F.when((F.col("AGE") >= 0) & (F.col("AGE") <= 49), 1)
         .when(F.col("AGE") >= 50, 2)
         .otherwise(-1)
    )
    .withColumn("AGE50PLUS_label",
        F.when(F.col("AGE50PLUS") == 1, "1 Age 0-49")
         .when(F.col("AGE50PLUS") == 2, "2 Age 50+")
         .otherwise("Unknown")
    )
    # RACETH: Race/Ethnicity
    .withColumn("RACETH",
        F.when(F.col("HISPANX") == 1, 1)  # Hispanic
         .when((F.col("HISPANX") == 2) & (F.col("RACEX") == 1), 2)  # White NH
         .when((F.col("HISPANX") == 2) & (F.col("RACEX") == 2), 3)  # Black NH
         .when((F.col("HISPANX") == 2) & (F.col("RACEX") == 4), 4)  # Asian NH
         .otherwise(5)  # Other NH
    )
    .withColumn("NEWRACE_label",
        F.when(F.col("RACETH") == 1, "1 Hispanic")
         .when(F.col("RACETH") == 2, "2 White NH")
         .when(F.col("RACETH") == 3, "3 Black NH")
         .when(F.col("RACETH") == 4, "4 Asian NH")
         .otherwise("5 Other NH")
    )
    # HIGHEDUC: Education level
    .withColumn("HIGHEDUC",
        F.when(F.col("AGE") < 16, 5)          # Too young
         .when(F.col("EDUCYR") < 0, 4)         # Unknown/refused
         .when(F.col("EDUCYR") < 12, 1)        # Less than HS
         .when(F.col("EDUCYR") == 12, 2)       # HS grad
         .when(F.col("EDUCYR") > 12, 3)        # Some college+
         .otherwise(-1)
    )
    .withColumn("HIGHEDUC_label",
        F.when(F.col("HIGHEDUC") == 1, "1 Less than HS")
         .when(F.col("HIGHEDUC") == 2, "2 HS Grad")
         .when(F.col("HIGHEDUC") == 3, "3 Some College+")
         .when(F.col("HIGHEDUC") == 4, "4 Unknown")
         .when(F.col("HIGHEDUC") == 5, "5 Under 16")
         .otherwise("Unknown")
    )
    # BOWEL53 label
    .withColumn("BOWEL53_label",
        F.when(F.col("BOWEL53") == 1, "1 Yes")
         .when(F.col("BOWEL53") == 2, "2 No")
         .otherwise("Other")
    )
)

# ---------------------------------------------------------------------------
# Figure 1: Colonoscopy by age category (Total)
# ---------------------------------------------------------------------------
print("=== Figure 1: Colonoscopy Screening by Age Category ===")
results_age = survey_freq(
    meps,
    table_vars=["AGECAT_label", "BOWEL53_label"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT05F"
)
print(results_age)

# ---------------------------------------------------------------------------
# Figure 2: Colonoscopy by race/ethnicity (among 50+)
# ---------------------------------------------------------------------------
print("\n=== Figure 2: Colonoscopy by Race/Ethnicity (Age 50+) ===")
meps_50plus = meps.filter(F.col("AGE50PLUS") == 2)
results_race = survey_freq(
    meps_50plus,
    table_vars=["NEWRACE_label", "BOWEL53_label"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT05F"
)
print(results_race)

# ---------------------------------------------------------------------------
# Figure 3: Colonoscopy by education (among 50+)
# ---------------------------------------------------------------------------
print("\n=== Figure 3: Colonoscopy by Education (Age 50+) ===")
results_educ = survey_freq(
    meps_50plus,
    table_vars=["HIGHEDUC_label", "BOWEL53_label"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT05F"
)
print(results_educ)

spark.stop()
