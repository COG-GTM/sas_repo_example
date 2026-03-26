"""
M1: Unweighted vs Weighted Frequency Comparison (2003 Data)

This example demonstrates the difference between unweighted and weighted
frequencies in the MEPS. The unweighted frequency represents the sample
while the weighted frequency represents the civilian noninstitutionalized
population.

Migrated from: SAS/older_exercises_1996_to_2006/Misc_examples/M1/M1.sas
Input file: h79.sas7bdat (2003 Full-Year Consolidated Data File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark

spark = get_spark()

# ---------------------------------------------------------------------------
# Load 2003 Full-Year Consolidated Data File (HC-079)
# ---------------------------------------------------------------------------
h79 = load_sas7bdat(spark, "C:/MEPS/h79.sas7bdat")

# ---------------------------------------------------------------------------
# Unweighted frequency of sex
# ---------------------------------------------------------------------------
print("=== Unweighted Frequency of Sex ===")
h79.groupBy("SEX").agg(
    F.count("*").alias("Unweighted_N")
).orderBy("SEX").show()

# ---------------------------------------------------------------------------
# Weighted frequency of sex
# ---------------------------------------------------------------------------
print("=== Weighted Frequency of Sex ===")
h79.groupBy("SEX").agg(
    F.sum("PERWT03F").alias("Weighted_N")
).orderBy("SEX").show()

# ---------------------------------------------------------------------------
# Unweighted frequency of race/ethnicity
# ---------------------------------------------------------------------------
print("=== Unweighted Frequency of Race/Ethnicity ===")
h79.groupBy("RACETHNX").agg(
    F.count("*").alias("Unweighted_N")
).orderBy("RACETHNX").show()

# ---------------------------------------------------------------------------
# Weighted frequency of race/ethnicity
# ---------------------------------------------------------------------------
print("=== Weighted Frequency of Race/Ethnicity ===")
h79.groupBy("RACETHNX").agg(
    F.sum("PERWT03F").alias("Weighted_N")
).orderBy("RACETHNX").show()

spark.stop()
