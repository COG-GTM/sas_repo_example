"""
L4: Linking Conditions to Full-Year File - Asthma Analysis (2001 Data)

This example shows how to:
  (1) Identify persons with specific condition(s) - asthma (ICD9=493)
  (2) Subset condition file to person level
  (3) Make one variable from five round-specific variables
  (4) Calculate survey-weighted estimates

Migrated from: SAS/older_exercises_1996_to_2006/Linking_examples/L4/L4.sas
Input files:
  h61.sas7bdat (2001 Conditions File)
  h60.sas7bdat (2001 Full-Year Persons File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.data_loader import load_sas7bdat, get_spark
from utils.survey_utils import survey_freq

spark = get_spark()

# ---------------------------------------------------------------------------
# Load data files
# ---------------------------------------------------------------------------
h61 = load_sas7bdat(spark, "C:/MEPS/h61.sas7bdat")  # 2001 Conditions
h60 = load_sas7bdat(spark, "C:/MEPS/h60.sas7bdat")   # 2001 Full-Year

# ---------------------------------------------------------------------------
# Identify persons with asthma (ICD9=493)
# ---------------------------------------------------------------------------
asthma_cond = h61.filter(F.col("ICD9CODX") == "493")

print("=== Asthma Conditions ===")
asthma_cond.select("DUPERSID", "CONDIDX", "ICD9CODX").show(10)

# ---------------------------------------------------------------------------
# Keep first-reported asthma record for each person
# ---------------------------------------------------------------------------
rn_window = Window.partitionBy("DUPERSID").orderBy("CONDRN")
asthma_first = (
    asthma_cond
    .withColumn("row_num", F.row_number().over(rn_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# ---------------------------------------------------------------------------
# Assign first-reported OVRALLi to OVERALL
# (How asthma affects overall health: 1=Very Serious ... 4=Not at All)
# ---------------------------------------------------------------------------
asthma_first = (
    asthma_first
    .withColumn("OVERALL",
        F.when(F.col("OVRALL1") > -1, F.col("OVRALL1"))
         .when(F.col("OVRALL2") > -1, F.col("OVRALL2"))
         .when(F.col("OVRALL3") > -1, F.col("OVRALL3"))
         .when(F.col("OVRALL4") > -1, F.col("OVRALL4"))
         .when(F.col("OVRALL5") > -1, F.col("OVRALL5"))
         .otherwise(5)  # Missing
    )
    .select("DUPERSID", "OVERALL")
)

# ---------------------------------------------------------------------------
# Merge with Full-Year file (all persons with positive weight)
# ---------------------------------------------------------------------------
pers = (
    h60
    .filter(F.col("PERWT01F") > 0)
    .join(asthma_first.withColumn("HAS_ASTHMA", F.lit(1)),
          on="DUPERSID", how="left")
    # Asthma flag
    .withColumn("ASTHMA",
        F.when(F.col("HAS_ASTHMA") == 1, 1).otherwise(2)
    )
    .withColumn("ASTHMA_label",
        F.when(F.col("ASTHMA") == 1, "1 Has Asthma")
         .when(F.col("ASTHMA") == 2, "2 No Asthma")
         .otherwise("Unknown")
    )
    # OVERALL: if no asthma, set to 6
    .withColumn("OVERALL",
        F.when(F.col("OVERALL").isNull(), 6).otherwise(F.col("OVERALL"))
    )
    .withColumn("OVERALL_label",
        F.when(F.col("OVERALL") == 1, "1 Very Serious")
         .when(F.col("OVERALL") == 2, "2 Somewhat Serious")
         .when(F.col("OVERALL") == 3, "3 Not Very Serious")
         .when(F.col("OVERALL") == 4, "4 Not at All")
         .when(F.col("OVERALL") == 5, "5 Missing")
         .when(F.col("OVERALL") == 6, "6 Not Have Asthma")
         .otherwise("Unknown")
    )
    # Age category
    .withColumn("AGE",
        F.when(F.col("AGE53X") >= 0, F.col("AGE53X"))
         .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
         .when(F.col("AGE31X") >= 0, F.col("AGE31X"))
         .otherwise(-1)
    )
    .withColumn("AGECAT",
        F.when((F.col("AGE") >= 0) & (F.col("AGE") < 18), 1)
         .when(F.col("AGE") >= 18, 2)
         .otherwise(-1)
    )
    .withColumn("AGECAT_label",
        F.when(F.col("AGECAT") == 1, "1. 0-17")
         .when(F.col("AGECAT") == 2, "2. 18+")
         .otherwise("Unknown")
    )
)

# ---------------------------------------------------------------------------
# Estimates: Prevalence of asthma
# ---------------------------------------------------------------------------
print("=== Asthma Prevalence ===")
results_asthma = survey_freq(
    pers,
    table_vars=["ASTHMA_label"],
    stratum_col="VARSTR01",
    cluster_col="VARPSU01",
    weight_col="PERWT01F"
)
print(results_asthma)

# ---------------------------------------------------------------------------
# Among persons with asthma: How it affects overall health, by age
# ---------------------------------------------------------------------------
print("\n=== Among Asthma Patients: Impact on Overall Health by Age ===")
asthma_persons = pers.filter(F.col("ASTHMA") == 1)
results_overall = survey_freq(
    asthma_persons,
    table_vars=["OVERALL_label", "AGECAT_label"],
    stratum_col="VARSTR01",
    cluster_col="VARPSU01",
    weight_col="PERWT01F"
)
print(results_overall)

spark.stop()
