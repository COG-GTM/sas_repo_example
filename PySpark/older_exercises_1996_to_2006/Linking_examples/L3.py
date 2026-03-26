"""
L3: Linking Event Files to Full-Year File (2001 Data)

This example shows how to:
  (1) Aggregate event records to the person level
  (2) Make 1 annual variable from 3 round variables
  (3) Make a categorical variable from a continuous variable
  (4) Calculate survey-weighted estimates

Migrated from: SAS/older_exercises_1996_to_2006/Linking_examples/L3/L3.sas
Input files:
  h59g.sas7bdat (2001 Office-Based Visits)
  h60.sas7bdat (2001 Full-Year File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_freq, survey_mean

spark = get_spark()

# ---------------------------------------------------------------------------
# Load data files
# ---------------------------------------------------------------------------
h59g = load_sas7bdat(spark, "C:/MEPS/h59g.sas7bdat")  # 2001 OB Visits
h60 = load_sas7bdat(spark, "C:/MEPS/h60.sas7bdat")    # 2001 Full-Year

# ---------------------------------------------------------------------------
# Identify persons with a visit for a general check-up & their expenditures
# Aggregate OB events to person level using Window functions
# ---------------------------------------------------------------------------
ob_window = Window.partitionBy("DUPERSID")

ob_person = (
    h59g
    # Flag general checkup visits (VSTCTGRY=1)
    .withColumn("IS_GENCKUP", F.when(F.col("VSTCTGRY") == 1, 1).otherwise(0))
    # Sum expenditures for general checkup visits only
    .withColumn("GENCKUP_TOTPD",
        F.when(F.col("VSTCTGRY") == 1, F.col("OBXP01X")).otherwise(0))
    .withColumn("GENCKUP_FAMPD",
        F.when(F.col("VSTCTGRY") == 1, F.col("OBSF01X")).otherwise(0))
    # Aggregate to person level
    .groupBy("DUPERSID")
    .agg(
        F.max("IS_GENCKUP").alias("GENCKUP"),
        F.sum("GENCKUP_TOTPD").alias("AMBTOTPD"),
        F.sum("GENCKUP_FAMPD").alias("AMBFAMPD")
    )
)

# Set GENCKUP: 1 if had checkup, missing/null if not
ob_person = ob_person.withColumn("GENCKUP",
    F.when(F.col("GENCKUP") == 1, 1).otherwise(F.lit(None))
)

print("=== Persons with General Checkup ===")
ob_person.groupBy("GENCKUP").count().show()

# ---------------------------------------------------------------------------
# Prepare Full-Year file: positive weight persons, construct age & insurance
# ---------------------------------------------------------------------------
fy = (
    h60
    .filter(F.col("PERWT01F") > 0)
    # Define AGE as last nonmissing age in 2001
    .withColumn("AGE",
        F.when(F.col("AGE53X") >= 0, F.col("AGE53X"))
         .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
         .when(F.col("AGE31X") >= 0, F.col("AGE31X"))
         .otherwise(-1)
    )
    # Make age category variable
    .withColumn("AGECAT",
        (F.when(F.col("AGE") >= 0, 1).otherwise(0) +
         F.when(F.col("AGE") > 3, 1).otherwise(0) +
         F.when(F.col("AGE") > 7, 1).otherwise(0) +
         F.when(F.col("AGE") > 11, 1).otherwise(0) +
         F.when(F.col("AGE") > 15, 1).otherwise(0) +
         F.when(F.col("AGE") > 17, 1).otherwise(0))
    )
    .withColumn("AGECAT_label",
        F.when(F.col("AGECAT") == 1, "1. 0-3")
         .when(F.col("AGECAT") == 2, "2. 4-7")
         .when(F.col("AGECAT") == 3, "3. 8-11")
         .when(F.col("AGECAT") == 4, "4. 12-15")
         .when(F.col("AGECAT") == 5, "5. 16-17")
         .when(F.col("AGECAT") == 6, "6. 18+")
         .otherwise("Unknown")
    )
    # Insurance status
    .withColumn("INSURED",
        F.when(F.col("INSCOV01") > 2, 2).otherwise(1)
    )
    .withColumn("INSURED_label",
        F.when(F.col("INSURED") == 1, "1 Insured")
         .when(F.col("INSURED") == 2, "2 Uninsured")
         .otherwise("Unknown")
    )
    .select("DUPERSID", "PERWT01F", "VARSTR01", "VARPSU01",
            "AGE", "AGECAT", "AGECAT_label", "INSURED", "INSURED_label")
)

# ---------------------------------------------------------------------------
# Link person-level events file with full-year person file
# ---------------------------------------------------------------------------
pers = (
    fy
    .join(ob_person, on="DUPERSID", how="left")
    # If no general checkup, set GENCKUP=2
    .withColumn("GENCKUP",
        F.when(F.col("GENCKUP").isNull(), 2).otherwise(F.col("GENCKUP"))
    )
    .withColumn("GENCKUP_label",
        F.when(F.col("GENCKUP") == 1, "1 General Checkup")
         .when(F.col("GENCKUP") == 2, "2 No General Checkup")
         .otherwise("Unknown")
    )
    .fillna({"AMBTOTPD": 0, "AMBFAMPD": 0})
)

# ---------------------------------------------------------------------------
# Survey estimates: Persons age 18+ with/without general checkup
# ---------------------------------------------------------------------------
adults = pers.filter(F.col("AGECAT") == 6)

print("=== Persons Age 18+: General Checkup by Insurance Status ===")
results_freq = survey_freq(
    adults,
    table_vars=["GENCKUP_label", "INSURED_label"],
    stratum_col="VARSTR01",
    cluster_col="VARPSU01",
    weight_col="PERWT01F"
)
print(results_freq)

# ---------------------------------------------------------------------------
# Mean expenditures for adults with general checkup by insurance status
# ---------------------------------------------------------------------------
print("\n=== Adults 18+ with General Checkup: Mean Expenditures ===")
adults_checkup = adults.filter(F.col("GENCKUP") == 1)
results_exp = survey_mean(
    adults_checkup,
    var_cols=["AMBTOTPD", "AMBFAMPD"],
    stratum_col="VARSTR01",
    cluster_col="VARPSU01",
    weight_col="PERWT01F",
    domain_col="INSURED_label"
)
print(results_exp)

spark.stop()
