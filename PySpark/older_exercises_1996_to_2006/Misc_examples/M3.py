"""
M3: ID Variables and Merging (2003 Data)

This example demonstrates how to use the MEPS ID variables (DUID, PID,
DUPERSID) to identify persons, families, and dwelling units, and how
to merge person-level data with event-level data.

Migrated from: SAS/older_exercises_1996_to_2006/Misc_examples/M3/M3.sas
Input files:
  h79.sas7bdat (2003 Full-Year Consolidated Data File)
  h77g.sas7bdat (2003 Office-Based Visits)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark

spark = get_spark()

# ---------------------------------------------------------------------------
# Load data files
# ---------------------------------------------------------------------------
h79 = load_sas7bdat(spark, "C:/MEPS/h79.sas7bdat")   # 2003 FYC
h77g = load_sas7bdat(spark, "C:/MEPS/h77g.sas7bdat")  # 2003 OB Visits

# ---------------------------------------------------------------------------
# Person-level: DUPERSID structure
# DUPERSID = DUID (7 chars) + PID (3 chars)
# ---------------------------------------------------------------------------
print("=== DUPERSID Structure ===")
person = (
    h79
    .withColumn("DUID_str", F.lpad(F.col("DUID").cast("string"), 7, "0"))
    .withColumn("PID_str", F.lpad(F.col("PID").cast("string"), 3, "0"))
    .withColumn("DUPERSID_check",
        F.concat(F.col("DUID_str"), F.col("PID_str")))
    .select("DUPERSID", "DUID", "PID", "DUPERSID_check")
)
person.show(10)

# ---------------------------------------------------------------------------
# Family-level variables
# FAMIDYR identifies families within a dwelling unit
# ---------------------------------------------------------------------------
print("=== Family-Level Variables ===")
fam_counts = (
    h79
    .groupBy("DUID", "FAMIDYR")
    .count()
    .withColumnRenamed("count", "FAM_SIZE")
)
fam_counts.orderBy("DUID", "FAMIDYR").show(20)

# ---------------------------------------------------------------------------
# Event-level: EVNTIDX structure
# EVNTIDX = DUID (7 chars) + PID (3 chars) + EVENT_NUMBER
# ---------------------------------------------------------------------------
print("=== Event-Level ID Variables ===")
events = (
    h77g
    .select("DUPERSID", "EVNTIDX", "OBXP03X")
)
events.show(10)

# ---------------------------------------------------------------------------
# Merge events back to person file for demographic characteristics
# ---------------------------------------------------------------------------
print("=== Event Records Merged with Person File ===")
merged = (
    events
    .join(
        h79.select("DUPERSID", "AGE03X", "SEX", "RACETHNX", "INSCOV03"),
        on="DUPERSID",
        how="inner"
    )
)

# Summary: number of OB visits per person
print("=== Number of OB Visits per Person (Sample) ===")
visit_counts = (
    merged
    .groupBy("DUPERSID")
    .agg(F.count("*").alias("N_VISITS"))
)
visit_counts.groupBy("N_VISITS").count().orderBy("N_VISITS").show(20)

# Total OB expenditures per person
print("=== Total OB Expenditures per Person ===")
person_exp = (
    merged
    .groupBy("DUPERSID")
    .agg(
        F.sum("OBXP03X").alias("TOTAL_OB_EXP"),
        F.count("*").alias("N_VISITS")
    )
)
person_exp.describe("TOTAL_OB_EXP", "N_VISITS").show()

spark.stop()
