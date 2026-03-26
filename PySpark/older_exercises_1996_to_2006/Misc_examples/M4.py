"""
M4: Counting Events from CLNK File (2003 Data)

This example demonstrates how to count the number of medical events
associated with conditions using the CLNK (Condition-Event Link) file.
It counts ER visits, hospital stays, office visits, outpatient visits,
and prescribed medicines linked to each condition.

Migrated from: SAS/older_exercises_1996_to_2006/Misc_examples/M4/M4.sas
Input files:
  h78if1.sas7bdat (2003 CLNK File)
  h79.sas7bdat (2003 Full-Year File) - for survey design variables
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
clnk = load_sas7bdat(spark, "C:/MEPS/h78if1.sas7bdat")  # 2003 CLNK
h79 = load_sas7bdat(spark, "C:/MEPS/h79.sas7bdat")      # 2003 FYC

# ---------------------------------------------------------------------------
# Identify event type from EVNTIDX
# The last 2 characters of EVNTIDX indicate the event type
# ---------------------------------------------------------------------------
clnk_typed = (
    clnk
    .withColumn("EVTYPE",
        F.substring(F.col("EVNTIDX"), -2, 2)
    )
    .withColumn("EVENT_TYPE",
        F.when(F.col("EVTYPE") == "ER", "ER Visit")
         .when(F.col("EVTYPE") == "IP", "Hospital Inpatient")
         .when(F.col("EVTYPE") == "OB", "Office-Based Visit")
         .when(F.col("EVTYPE") == "OP", "Outpatient Visit")
         .when(F.col("EVTYPE") == "HH", "Home Health")
         .when(F.col("EVTYPE") == "PM", "Prescribed Medicine")
         .otherwise("Other")
    )
)

# ---------------------------------------------------------------------------
# Count of events by type across all conditions
# ---------------------------------------------------------------------------
print("=== Total Events by Type (All Conditions) ===")
clnk_typed.groupBy("EVENT_TYPE").count().orderBy("EVENT_TYPE").show()

# ---------------------------------------------------------------------------
# Count events per condition
# ---------------------------------------------------------------------------
cond_events = (
    clnk_typed
    .groupBy("CONDIDX", "DUPERSID")
    .agg(
        F.count("*").alias("TOTAL_EVENTS"),
        F.sum(F.when(F.col("EVTYPE") == "ER", 1).otherwise(0)).alias("N_ER"),
        F.sum(F.when(F.col("EVTYPE") == "IP", 1).otherwise(0)).alias("N_IP"),
        F.sum(F.when(F.col("EVTYPE") == "OB", 1).otherwise(0)).alias("N_OB"),
        F.sum(F.when(F.col("EVTYPE") == "OP", 1).otherwise(0)).alias("N_OP"),
        F.sum(F.when(F.col("EVTYPE") == "PM", 1).otherwise(0)).alias("N_PM"),
        F.sum(F.when(F.col("EVTYPE") == "HH", 1).otherwise(0)).alias("N_HH")
    )
)

print("=== Distribution of Total Events per Condition ===")
cond_events.describe("TOTAL_EVENTS", "N_ER", "N_IP", "N_OB",
                      "N_OP", "N_PM", "N_HH").show()

# ---------------------------------------------------------------------------
# Aggregate to person level: total events per person
# ---------------------------------------------------------------------------
person_events = (
    clnk_typed
    .groupBy("DUPERSID")
    .agg(
        F.count("*").alias("TOTAL_EVENTS"),
        F.countDistinct("CONDIDX").alias("N_CONDITIONS"),
        F.sum(F.when(F.col("EVTYPE") == "ER", 1).otherwise(0)).alias("N_ER"),
        F.sum(F.when(F.col("EVTYPE") == "IP", 1).otherwise(0)).alias("N_IP"),
        F.sum(F.when(F.col("EVTYPE") == "OB", 1).otherwise(0)).alias("N_OB"),
        F.sum(F.when(F.col("EVTYPE") == "OP", 1).otherwise(0)).alias("N_OP"),
        F.sum(F.when(F.col("EVTYPE") == "PM", 1).otherwise(0)).alias("N_PM")
    )
)

print("=== Person-Level Event Counts ===")
person_events.describe("TOTAL_EVENTS", "N_CONDITIONS", "N_ER",
                        "N_IP", "N_OB", "N_OP", "N_PM").show()

# ---------------------------------------------------------------------------
# Merge with FY file for weighting
# ---------------------------------------------------------------------------
person_merged = (
    h79
    .select("DUPERSID", "PERWT03F")
    .join(person_events, on="DUPERSID", how="left")
    .fillna({
        "TOTAL_EVENTS": 0, "N_CONDITIONS": 0,
        "N_ER": 0, "N_IP": 0, "N_OB": 0, "N_OP": 0, "N_PM": 0
    })
)

print("=== Weighted Mean Events per Person ===")
person_merged.filter(F.col("PERWT03F") > 0).select(
    F.avg("TOTAL_EVENTS").alias("Mean_Events"),
    F.avg("N_CONDITIONS").alias("Mean_Conditions")
).show()

spark.stop()
