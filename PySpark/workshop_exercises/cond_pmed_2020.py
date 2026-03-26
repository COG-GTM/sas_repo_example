"""
Condition-Event Linkage: Prescribed Medicine Utilization and Expenditures
for the Treatment of Hyperlipidemia, 2020

Links MEPS-HC Medical Conditions file to the Prescribed Medicines file.

National totals:
  - Total number of people w/ at least one PMED fill for hyperlipidemia (HL)
  - Total PMED fills for HL
  - Total PMED expenditures for HL

Per-person averages among people with at least one PMED fill for HL:
  - Avg PMED fills for HL, by sex and poverty (POVCAT20)
  - Avg PMED expenditures for HL, by sex and poverty (POVCAT20)

Input files:
  - C:/MEPS/h220a.sas7bdat   (2020 Prescribed Medicines file)
  - C:/MEPS/h222.sas7bdat    (2020 Conditions file)
  - C:/MEPS/h220if1.sas7bdat (2020 CLNK: Condition-Event Link file)
  - C:/MEPS/h224.sas7bdat    (2020 Full-Year Consolidated file)

Replaces: SAS/workshop_exercises/cond_pmed_2020.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# ============================================================================
# Read in data files and keep only needed variables
# ============================================================================

# PMED file (record = rx fill or refill for a person)
pmed20 = (
    load_sas7bdat(spark, "C:/MEPS/h220a.sas7bdat")
    .withColumn("EVNTIDX", F.col("LINKIDX"))  # rename LINKIDX to EVNTIDX for merging
    .select("DUPERSID", "DRUGIDX", "RXRECIDX", "EVNTIDX", "RXDRGNAM", "RXXP20X")
)

# Conditions file (record = medical condition for a person)
cond20 = (
    load_sas7bdat(spark, "C:/MEPS/h222.sas7bdat")
    .select("DUPERSID", "CONDIDX", "ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X")
)

# Conditions-event link file
clnk20 = load_sas7bdat(spark, "C:/MEPS/h220if1.sas7bdat")

# Full-year consolidated (person-level) file
fyc20 = (
    load_sas7bdat(spark, "C:/MEPS/h224.sas7bdat")
    .select("DUPERSID", "AGELAST", "SEX", "POVCAT20", "CHOLDX",
            "PERWT20F", "VARPSU", "VARSTR")
)

# ============================================================================
# Subset conditions file to hyperlipidemia (any CCSR = "END010")
# ============================================================================
hl = cond20.filter(
    (F.col("CCSR1X") == "END010") |
    (F.col("CCSR2X") == "END010") |
    (F.col("CCSR3X") == "END010")
)

# Show duplicate example (illustration only)
print("=== Example: duplicate HL conditions ===")
hl_dup_check = hl.groupBy("DUPERSID").count().filter(F.col("count") > 1)
if hl_dup_check.count() > 0:
    example_id = hl_dup_check.first()["DUPERSID"]
    hl.filter(F.col("DUPERSID") == example_id).show(truncate=False)

# ============================================================================
# Get EVNTIDX values for HL records from CLNK file
# ============================================================================
clnk_hl = (
    hl.select("DUPERSID", "CONDIDX")
    .join(clnk20, on=["DUPERSID", "CONDIDX"], how="inner")
)

# De-duplicate by EVNTIDX (don't double-count same PMEDs)
clnk_hl_dedup = clnk_hl.dropDuplicates(["DUPERSID", "EVNTIDX"])

# Check event types
print("=== Event types linked to HL ===")
clnk_hl_dedup.groupBy("EVENTYPE").count().show()

# ============================================================================
# Get PMED events linked to hyperlipidemia
# ============================================================================
hl_merged = (
    clnk_hl_dedup
    .join(pmed20, on=["DUPERSID", "EVNTIDX"], how="inner")
)

# QC: All events should have EVENTYPE = 8 (PMED)
print("=== QC: Event types (should all be 8) ===")
hl_merged.groupBy("EVENTYPE").count().show()

# QC: Top drugs for HL (by unweighted fills)
print("=== Top 10 drugs for hyperlipidemia ===")
hl_merged.groupBy("RXDRGNAM").count().orderBy(F.desc("count")).show(10, truncate=False)

# Create dummy variable for each fill
hl_merged = hl_merged.withColumn("HL_FILL", F.lit(1))

# ============================================================================
# Roll up to person level
# ============================================================================
drugs_by_pers = (
    hl_merged
    .groupBy("DUPERSID")
    .agg(
        F.sum("HL_FILL").alias("N_HL_FILLS"),
        F.sum("RXXP20X").alias("HL_DRUG_EXP"),
    )
)

# ============================================================================
# Merge person-level totals back to FYC
# ============================================================================
fyc_hl = (
    fyc20
    .join(drugs_by_pers, on="DUPERSID", how="left")
    # Create flag for people with any PMED fills for HL
    .withColumn("HL_PMED_FLAG",
        F.when(F.col("N_HL_FILLS") > 0, 1).otherwise(0))
    # Set missings to zero
    .fillna({"N_HL_FILLS": 0, "HL_DRUG_EXP": 0})
)

# QC: Compare ever-diagnosed (CHOLDX=1) vs current PMED users
print("=== QC: CHOLDX vs HL_PMED_FLAG ===")
fyc_hl.groupBy("CHOLDX", "HL_PMED_FLAG").count().orderBy("CHOLDX", "HL_PMED_FLAG").show()

print("=== QC: HL_PMED_FLAG distribution ===")
fyc_hl.groupBy("HL_PMED_FLAG").count().show()

# QC: No records with flag=0 and positive exp or fills
print("=== QC: Should be empty ===")
fyc_hl.filter((F.col("HL_PMED_FLAG") == 0) &
              ((F.col("HL_DRUG_EXP") > 0) | (F.col("N_HL_FILLS") > 0))).show()

# ============================================================================
# ESTIMATION
# ============================================================================

# --- National Totals ---
print("=== National Totals ===")
print("sum(HL_PMED_FLAG) = total people with any rx fills for HL")
print("sum(N_HL_FILLS) = total number of rx fills for HL")
print("sum(HL_DRUG_EXP) = total rx expenditures for HL")

results_totals = survey_total(
    fyc_hl,
    var_cols=["HL_PMED_FLAG", "N_HL_FILLS", "HL_DRUG_EXP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT20F",
)
print(results_totals.to_string())

# --- Per-person averages for people with HL PMED fills, by sex and poverty ---
print("\n=== Per-person averages (HL_PMED_FLAG=1), overall ===")
results_mean_overall = survey_mean(
    fyc_hl,
    var_cols=["N_HL_FILLS", "HL_DRUG_EXP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT20F",
    domain_col="HL_PMED_FLAG",
)
print(results_mean_overall.to_string())

# By sex
fyc_hl = fyc_hl.withColumn("SEX_label",
    F.when(F.col("SEX") == 1, "Male").when(F.col("SEX") == 2, "Female"))

print("\n=== Per-person averages by sex (among HL PMED users) ===")
fyc_hl_sub = fyc_hl.filter(F.col("HL_PMED_FLAG") == 1)
results_sex = survey_mean(
    fyc_hl_sub,
    var_cols=["N_HL_FILLS", "HL_DRUG_EXP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT20F",
    domain_col="SEX_label",
)
print(results_sex.to_string())

# By poverty status
fyc_hl = fyc_hl.withColumn("POVCAT20_label",
    F.when(F.col("POVCAT20") == 1, "1 Negative/Poor")
    .when(F.col("POVCAT20") == 2, "2 Near-Poor")
    .when(F.col("POVCAT20") == 3, "3 Low Income")
    .when(F.col("POVCAT20") == 4, "4 Middle Income")
    .when(F.col("POVCAT20") == 5, "5 High Income"))

print("\n=== Per-person averages by poverty (among HL PMED users) ===")
fyc_hl_sub2 = fyc_hl.filter(F.col("HL_PMED_FLAG") == 1)
results_pov = survey_mean(
    fyc_hl_sub2,
    var_cols=["N_HL_FILLS", "HL_DRUG_EXP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT20F",
    domain_col="POVCAT20_label",
)
print(results_pov.to_string())

spark.stop()
