"""
Condition-Event Linkage Example: Office-Based Mental Health Visits, 2020

Links MEPS-HC Medical Conditions file to Office-based medical visits file.

Event-level estimates:
  - Number of office-based visits for mental health:       343,810,085 (SE: 22,252,863)
  - Total exp. for office-based mental health visits:  $60,209,392,314 (SE: 4,437,433,004)
  - Mean exp. per visit:                                       $175.12 (SE: 6.46)

Person-level estimates:
  - Number of people with office visit for MH:  29,816,984 (SE: 1,192,676)
  - Percent of people with office visit for MH:      9.08% (SE: 0.29%)
  - Mean exp per person for office visits for MH: $2019.30 (SE: 126.16)

Input files:
  - C:/MEPS/h220g.sas7bdat   (2020 Office-based event file)
  - C:/MEPS/h222.sas7bdat    (2020 Conditions file)
  - C:/MEPS/h220if1.sas7bdat (2020 CLNK: Condition-event link file)
  - C:/MEPS/h224.sas7bdat    (2020 Full-Year Consolidated file)

Replaces: SAS/workshop_exercises/cond_mv_2020.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# ============================================================================
# Load datasets
# ============================================================================
ob20 = load_sas7bdat(spark, "C:/MEPS/h220g.sas7bdat")     # Office-based visits
cond20 = load_sas7bdat(spark, "C:/MEPS/h222.sas7bdat")    # Medical conditions
clnk20 = load_sas7bdat(spark, "C:/MEPS/h220if1.sas7bdat") # Condition-event link
fyc20 = load_sas7bdat(spark, "C:/MEPS/h224.sas7bdat")     # Person-level FYC

# Preview files
print("=== Office-based visits (first 5) ===")
ob20.show(5, truncate=False)
print("=== Conditions (first 5) ===")
cond20.show(5, truncate=False)
print("=== Condition-event link (first 5) ===")
clnk20.show(5, truncate=False)

# ============================================================================
# Keep only needed variables
# ============================================================================
ob20x = ob20.select("PANEL", "DUPERSID", "EVNTIDX", "EVENTRN",
                     "OBXP20X", "PERWT20F", "VARSTR", "VARPSU",
                     "TELEHEALTHFLAG")

cond20x = cond20.select("DUPERSID", "CONDIDX", "ICD10CDX",
                         "CCSR1X", "CCSR2X", "CCSR3X")

fyc20x = fyc20.select("DUPERSID", "PERWT20F", "VARSTR", "VARPSU")

# ============================================================================
# Filter COND file to only people with Mental Disorders
# ============================================================================
# Mental health CCSR codes: MBD*, FAC002, FAC007, NVS011, SYM008, SYM009
cond20x = cond20x.withColumn("ALL_CCSR",
    F.concat(
        F.coalesce(F.col("CCSR1X"), F.lit("")),
        F.coalesce(F.col("CCSR2X"), F.lit("")),
        F.coalesce(F.col("CCSR3X"), F.lit(""))
    ))

mental_health = cond20x.filter(
    F.col("ALL_CCSR").contains("MBD") |
    F.col("ALL_CCSR").contains("FAC002") |
    F.col("ALL_CCSR").contains("FAC007") |
    F.col("ALL_CCSR").contains("NVS011") |
    F.col("ALL_CCSR").contains("SYM008") |
    F.col("ALL_CCSR").contains("SYM009")
)

print("=== Mental health conditions ===")
mental_health.groupBy("ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X").count().show(20, truncate=False)

# ============================================================================
# Filter CLNK file to only office-based visits (EVENTYPE = 1)
# ============================================================================
clnk_ob = clnk20.filter(F.col("EVENTYPE") == 1)

print("=== CLNK Office-based visits only ===")
clnk_ob.groupBy("EVENTYPE").count().show()

# ============================================================================
# Merge conditions with CLNK (inner join on DUPERSID, CONDIDX)
# ============================================================================
mh_clnk = (
    mental_health.select("DUPERSID", "CONDIDX")
    .join(clnk_ob, on=["DUPERSID", "CONDIDX"], how="inner")
)

# De-duplicate by event ID (EVNTIDX) - don't count same event twice
mh_clnk_nodup = (
    mh_clnk
    .select("DUPERSID", "EVNTIDX", "EVENTYPE")
    .dropDuplicates(["DUPERSID", "EVNTIDX", "EVENTYPE"])
)

# ============================================================================
# Merge on event files (office-based visits)
# ============================================================================
ob_mental_health = (
    mh_clnk_nodup
    .join(ob20x, on=["DUPERSID", "EVNTIDX"], how="inner")
    .withColumn("MH_OB_VISIT", F.lit(1))
)

# QC
print("=== ob_mental_health (first 5) ===")
ob_mental_health.show(5, truncate=False)
ob_mental_health.groupBy("EVENTYPE", "MH_OB_VISIT").count().show()

# ============================================================================
# Merge on FYC file for complete Strata, PSUs
# ============================================================================
# Full outer approach: keep all FYC persons
ob_mh_fyc = (
    fyc20x
    .join(
        ob_mental_health.select("DUPERSID", "EVNTIDX", "OBXP20X", "MH_OB_VISIT"),
        on="DUPERSID",
        how="left"
    )
    .withColumn("MH_OB", F.when(F.col("MH_OB_VISIT").isNotNull(), 1).otherwise(0))
    .fillna({"MH_OB_VISIT": 0, "OBXP20X": 0})
)

# QC
print("=== ob_mh_fyc QC ===")
ob_mh_fyc.groupBy("MH_OB", "MH_OB_VISIT").count().show()

# ============================================================================
# Event-level estimates
# ============================================================================
print("=== Event-level estimates ===")
print("Expected: Num visits=343,810,085 (SE: 22,252,863)")
print("Expected: Total exp=$60,209,392,314 (SE: 4,437,433,004)")
print("Expected: Mean exp/visit=$175.12 (SE: 6.46)")

results_event = survey_mean(
    ob_mh_fyc,
    var_cols=["MH_OB_VISIT", "OBXP20X"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT20F",
    domain_col="MH_OB",
)
print(results_event.to_string())

results_event_total = survey_total(
    ob_mh_fyc,
    var_cols=["MH_OB_VISIT", "OBXP20X"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT20F",
    domain_col="MH_OB",
)
print(results_event_total.to_string())

# ============================================================================
# Person-level estimates
# ============================================================================
# Aggregate to person-level
pers_mh = (
    ob_mh_fyc
    .groupBy("DUPERSID", "VARSTR", "VARPSU")
    .agg(
        F.mean("PERWT20F").alias("PERWT20F"),
        F.sum("OBXP20X").alias("PERSXP"),
        F.sum("MH_OB_VISIT").alias("PERS_NEVENTS"),
        F.mean("MH_OB_VISIT").alias("MH_OB_VISIT_PERS"),
        F.mean("MH_OB").alias("MH_OB_PERS"),
    )
)

# QC
print("\n=== Person-level QC ===")
pers_mh.groupBy("MH_OB_PERS").count().show()

# Person-level survey estimates
print("=== Person-level estimates ===")
print("Expected: Num people=29,816,984 (SE: 1,192,676)")
print("Expected: Pct people=9.08% (SE: 0.29%)")
print("Expected: Mean exp/person=$2019.30 (SE: 126.16)")

results_pers = survey_mean(
    pers_mh,
    var_cols=["MH_OB_VISIT_PERS", "PERSXP", "PERS_NEVENTS"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT20F",
    domain_col="MH_OB_PERS",
)
print(results_pers.to_string())

results_pers_total = survey_total(
    pers_mh,
    var_cols=["MH_OB_VISIT_PERS", "PERSXP", "PERS_NEVENTS"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT20F",
    domain_col="MH_OB_PERS",
)
print(results_pers_total.to_string())

spark.stop()
