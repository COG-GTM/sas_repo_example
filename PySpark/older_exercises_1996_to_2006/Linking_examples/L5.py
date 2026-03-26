"""
L5: Linking Conditions to Event Files - Asthma Expenditures (2001 Data)

This example shows how to:
  (1) Use condition file to identify events for asthma
  (2) Link conditions to events via CLNK file
  (3) Merge condition-event linked file to event files
  (4) Construct standardized expenditure variables across event types
  (5) Combine event files and identify type of event
  (6) Aggregate event-level records to person level

Migrated from: SAS/older_exercises_1996_to_2006/Linking_examples/L5/L5.sas
Input files:
  h61.sas7bdat (2001 Conditions), h59if1.sas7bdat (2001 CLNK),
  h59a.sas7bdat (2001 PMED), h59e.sas7bdat (2001 ER),
  h59f.sas7bdat (2001 Outpatient), h59g.sas7bdat (2001 Office-Based),
  h60.sas7bdat (2001 Full-Year Persons)
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
h61 = load_sas7bdat(spark, "C:/MEPS/h61.sas7bdat")     # 2001 Conditions
clnk = load_sas7bdat(spark, "C:/MEPS/h59if1.sas7bdat")  # 2001 CLNK
h59a = load_sas7bdat(spark, "C:/MEPS/h59a.sas7bdat")    # 2001 PMED
h59e = load_sas7bdat(spark, "C:/MEPS/h59e.sas7bdat")    # 2001 ER
h59f = load_sas7bdat(spark, "C:/MEPS/h59f.sas7bdat")    # 2001 Outpatient
h59g = load_sas7bdat(spark, "C:/MEPS/h59g.sas7bdat")    # 2001 Office-Based
h60 = load_sas7bdat(spark, "C:/MEPS/h60.sas7bdat")      # 2001 Full-Year

# ---------------------------------------------------------------------------
# Identify asthma conditions (ICD9=493)
# ---------------------------------------------------------------------------
cond = (
    h61
    .filter(F.col("ICD9CODX") == "493")
    .select("CONDIDX", "DUPERSID")
)

# ---------------------------------------------------------------------------
# Merge conditions with CLNK to get associated event IDs
# ---------------------------------------------------------------------------
condev = (
    cond
    .join(clnk, on="CONDIDX", how="inner")
    .dropDuplicates(["EVNTIDX"])
)

# ---------------------------------------------------------------------------
# Prescribed Medicines: LINKIDX maps to EVNTIDX
# ---------------------------------------------------------------------------
pmed = (
    h59a
    .withColumnRenamed("LINKIDX", "EVNTIDX")
    .withColumn("AMBTOTEV", F.col("RXXP01X"))
    .withColumn("AMBFAMEV", F.col("RXSF01X"))
    .select("EVNTIDX", "DUPERSID", "AMBTOTEV", "AMBFAMEV")
)

pm_events = (
    condev
    .join(pmed, on=["EVNTIDX", "DUPERSID"], how="inner")
    .withColumn("VISTYPE", F.lit("pm"))
)

# ---------------------------------------------------------------------------
# Office-Based Visits
# ---------------------------------------------------------------------------
ob = (
    h59g
    .withColumn("AMBTOTEV", F.col("OBXP01X"))
    .withColumn("AMBFAMEV", F.col("OBSF01X"))
    .select("EVNTIDX", "DUPERSID", "AMBTOTEV", "AMBFAMEV")
)

ob_events = (
    condev
    .join(ob, on=["EVNTIDX", "DUPERSID"], how="inner")
    .withColumn("VISTYPE", F.lit("ob"))
)

# ---------------------------------------------------------------------------
# Outpatient Visits (combine facility + doctor amounts)
# ---------------------------------------------------------------------------
op = (
    h59f
    .withColumn("AMBTOTEV", F.col("OPXP01X"))
    .withColumn("AMBFAMEV", F.col("OPFSF01X") + F.col("OPDSF01X"))
    .select("EVNTIDX", "DUPERSID", "AMBTOTEV", "AMBFAMEV")
)

op_events = (
    condev
    .join(op, on=["EVNTIDX", "DUPERSID"], how="inner")
    .withColumn("VISTYPE", F.lit("op"))
)

# ---------------------------------------------------------------------------
# Emergency Room Visits (combine facility + doctor amounts)
# ---------------------------------------------------------------------------
er = (
    h59e
    .withColumn("AMBTOTEV", F.col("ERXP01X"))
    .withColumn("AMBFAMEV", F.col("ERFSF01X") + F.col("ERDSF01X"))
    .select("EVNTIDX", "DUPERSID", "AMBTOTEV", "AMBFAMEV")
)

er_events = (
    condev
    .join(er, on=["EVNTIDX", "DUPERSID"], how="inner")
    .withColumn("VISTYPE", F.lit("er"))
)

# ---------------------------------------------------------------------------
# Combine all event files
# ---------------------------------------------------------------------------
common_cols = ["EVNTIDX", "DUPERSID", "AMBTOTEV", "AMBFAMEV", "VISTYPE"]
allevnt = (
    ob_events.select(common_cols)
    .unionByName(op_events.select(common_cols))
    .unionByName(er_events.select(common_cols))
    .unionByName(pm_events.select(common_cols))
)

print("=== Frequency of Ambulatory Visits for Asthma by Type ===")
allevnt.groupBy("VISTYPE").count().orderBy("VISTYPE").show()

# ---------------------------------------------------------------------------
# Aggregate events to person level
# ---------------------------------------------------------------------------
perev = (
    allevnt
    .groupBy("DUPERSID")
    .agg(
        F.sum("AMBTOTEV").alias("AMBTOTPD"),
        F.sum("AMBFAMEV").alias("AMBFAMPD")
    )
)

# ---------------------------------------------------------------------------
# Add person characteristics from Full-Year file
# ---------------------------------------------------------------------------
pers = (
    perev
    .join(
        h60.select("DUPERSID", "PERWT01F", "AGE31X", "AGE42X", "AGE53X",
                    "SEX", "RACETHNX"),
        on="DUPERSID",
        how="inner"
    )
    # Construct latest age
    .withColumn("AGE",
        F.when(F.col("AGE53X") >= 0, F.col("AGE53X"))
         .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
         .when(F.col("AGE31X") >= 0, F.col("AGE31X"))
         .otherwise(-1)
    )
    .withColumn("AGE_label",
        F.when((F.col("AGE") >= 0) & (F.col("AGE") <= 4), "0-4")
         .when((F.col("AGE") >= 5) & (F.col("AGE") <= 17), "5-17")
         .when((F.col("AGE") >= 18) & (F.col("AGE") <= 24), "18-24")
         .when((F.col("AGE") >= 25) & (F.col("AGE") <= 44), "25-44")
         .when((F.col("AGE") >= 45) & (F.col("AGE") <= 64), "45-64")
         .when(F.col("AGE") >= 65, "65-90")
         .otherwise("Unknown")
    )
    .withColumn("RACETHNX_label",
        F.when(F.col("RACETHNX") == 1, "Hispanic")
         .when(F.col("RACETHNX") == 2, "Black, not Hispanic")
         .otherwise("Other")
    )
    .withColumn("SEX_label",
        F.when(F.col("SEX") == 1, "Male")
         .when(F.col("SEX") == 2, "Female")
         .otherwise("Unknown")
    )
)

# ---------------------------------------------------------------------------
# Prescription-only expenditures per person for asthma
# ---------------------------------------------------------------------------
perpmed = (
    pm_events
    .groupBy("DUPERSID")
    .agg(
        F.sum("AMBTOTEV").alias("PMED_TOTPD"),
        F.sum("AMBFAMEV").alias("PMED_FAMPD")
    )
)

pers_pmed = (
    perpmed
    .join(h60.select("DUPERSID", "INSCOV01", "PERWT01F"), on="DUPERSID", how="inner")
    .withColumn("INSCOV01_label",
        F.when(F.col("INSCOV01") == 1, "Any private")
         .when(F.col("INSCOV01") == 2, "Public only")
         .when(F.col("INSCOV01") == 3, "Uninsured")
         .otherwise("Unknown")
    )
)

print("=== Average Rx Expenditures per Person for Asthma by Insurance ===")
results_pmed = survey_mean(
    pers_pmed,
    var_cols=["PMED_TOTPD", "PMED_FAMPD"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT01F",
    domain_col="INSCOV01_label"
)
print(results_pmed)

# ---------------------------------------------------------------------------
# All ambulatory expenditures per person for asthma
# ---------------------------------------------------------------------------
print("\n=== Average All Ambulatory Expenditures per Person for Asthma ===")
print("By Age:")
pers.groupBy("AGE_label").agg(
    F.avg("AMBTOTPD").alias("Mean_Total"),
    F.avg("AMBFAMPD").alias("Mean_Family")
).orderBy("AGE_label").show()

print("By Race/Ethnicity:")
pers.groupBy("RACETHNX_label").agg(
    F.avg("AMBTOTPD").alias("Mean_Total"),
    F.avg("AMBFAMPD").alias("Mean_Family")
).orderBy("RACETHNX_label").show()

print("By Sex:")
pers.groupBy("SEX_label").agg(
    F.avg("AMBTOTPD").alias("Mean_Total"),
    F.avg("AMBFAMPD").alias("Mean_Family")
).orderBy("SEX_label").show()

spark.stop()
