"""
Exercise 3b: Expenditures for All Events Associated with Diabetes, 2015

Illustrates how to calculate expenditures for all events associated with
a condition (diabetes, CCS codes 049/050), including:
  - All types of service combined
  - By type of service (ambulatory, ER, inpatient, home health, PMED)

Input files:
  - C:/MEPS/h181.sas7bdat (2015 FY PUF)
  - C:/MEPS/h180.sas7bdat (2015 Condition PUF)
  - C:/MEPS/h178a.sas7bdat (2015 PMED PUF)
  - C:/MEPS/h178d.sas7bdat (2015 Inpatient PUF)
  - C:/MEPS/h178e.sas7bdat (2015 ER PUF)
  - C:/MEPS/h178f.sas7bdat (2015 Outpatient PUF)
  - C:/MEPS/h178g.sas7bdat (2015 Office-Based PUF)
  - C:/MEPS/h178h.sas7bdat (2015 Home Health PUF)
  - C:/MEPS/h178if1.sas7bdat (2015 Condition-Event Link PUF)

Replaces: SAS/workshop_exercises/exercise_3b/Exercise3b.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# 1) Pull out conditions with diabetes
h180 = load_sas7bdat(spark, "C:/MEPS/h180.sas7bdat")
diab = h180.filter(F.col("CCCODEX").isin("049", "050"))

# 2) Get event IDs from condition-event link file
clnk = load_sas7bdat(spark, "C:/MEPS/h178if1.sas7bdat")
diab2 = (
    diab.select("DUPERSID", "CONDIDX", "CCCODEX")
    .join(clnk.select("CONDIDX", "EVNTIDX"), on="CONDIDX", how="inner")
)

# 3) Delete duplicate cases per event
diab3 = diab2.select("DUPERSID", "EVNTIDX").dropDuplicates(["EVNTIDX"])

# 4) Sum up PMED purchase-level data to event-level
pmed = (
    load_sas7bdat(spark, "C:/MEPS/h178a.sas7bdat")
    .withColumnRenamed("LINKIDX", "EVNTIDX")
)
pmed2 = pmed.groupBy("EVNTIDX").agg(
    F.sum("RXXP15X").alias("TOTEXP"),
    F.sum("RXSF15X").alias("SF"),
    F.sum("RXMR15X").alias("MR"),
    F.sum("RXMD15X").alias("MD"),
    F.sum("RXPV15X").alias("PV"),
    F.sum("RXVA15X").alias("VA"),
    F.sum("RXTR15X").alias("TR"),
    F.sum("RXOF15X").alias("OF"),
    F.sum("RXSL15X").alias("SL"),
    F.sum("RXWC15X").alias("WC"),
    F.sum("RXOR15X").alias("OR"),
    F.sum("RXOU15X").alias("OU"),
    F.sum("RXOT15X").alias("OT"),
)
pmed3 = pmed2.filter(F.col("TOTEXP") >= 0).withColumn("EVNTYP", F.lit("PMED"))

# 5) Align expenditure variables across event types
sop_cols = ["EVNTIDX", "SF", "MR", "MD", "PV", "VA", "TR", "OF", "SL", "WC", "OR", "OU", "OT", "TOTEXP", "EVNTYP"]

ob = (
    load_sas7bdat(spark, "C:/MEPS/h178g.sas7bdat")
    .withColumn("SF", F.col("OBSF15X")).withColumn("MR", F.col("OBMR15X"))
    .withColumn("MD", F.col("OBMD15X")).withColumn("PV", F.col("OBPV15X"))
    .withColumn("VA", F.col("OBVA15X")).withColumn("TR", F.col("OBTR15X"))
    .withColumn("OF", F.col("OBOF15X")).withColumn("SL", F.col("OBSL15X"))
    .withColumn("WC", F.col("OBWC15X")).withColumn("OR", F.col("OBOR15X"))
    .withColumn("OU", F.col("OBOU15X")).withColumn("OT", F.col("OBOT15X"))
    .withColumn("TOTEXP", F.col("OBXP15X"))
    .withColumn("EVNTYP", F.lit("AMBU"))
    .filter(F.col("TOTEXP") >= 0)
    .select(sop_cols)
)

erom = (
    load_sas7bdat(spark, "C:/MEPS/h178e.sas7bdat")
    .withColumn("SF", F.col("ERFSF15X") + F.col("ERDSF15X"))
    .withColumn("MR", F.col("ERFMR15X") + F.col("ERDMR15X"))
    .withColumn("MD", F.col("ERFMD15X") + F.col("ERDMD15X"))
    .withColumn("PV", F.col("ERFPV15X") + F.col("ERDPV15X"))
    .withColumn("VA", F.col("ERFVA15X") + F.col("ERDVA15X"))
    .withColumn("TR", F.col("ERFTR15X") + F.col("ERDTR15X"))
    .withColumn("OF", F.col("ERFOF15X") + F.col("ERDOF15X"))
    .withColumn("SL", F.col("ERFSL15X") + F.col("ERDSL15X"))
    .withColumn("WC", F.col("ERFWC15X") + F.col("ERDWC15X"))
    .withColumn("OR", F.col("ERFOR15X") + F.col("ERDOR15X"))
    .withColumn("OU", F.col("ERFOU15X") + F.col("ERDOU15X"))
    .withColumn("OT", F.col("ERFOT15X") + F.col("ERDOT15X"))
    .withColumn("TOTEXP", F.col("ERXP15X"))
    .withColumn("EVNTYP", F.lit("EROM"))
    .filter(F.col("TOTEXP") >= 0)
    .select(sop_cols)
)

ipat = (
    load_sas7bdat(spark, "C:/MEPS/h178d.sas7bdat")
    .withColumn("SF", F.col("IPFSF15X") + F.col("IPDSF15X"))
    .withColumn("MR", F.col("IPFMR15X") + F.col("IPDMR15X"))
    .withColumn("MD", F.col("IPFMD15X") + F.col("IPDMD15X"))
    .withColumn("PV", F.col("IPFPV15X") + F.col("IPDPV15X"))
    .withColumn("VA", F.col("IPFVA15X") + F.col("IPDVA15X"))
    .withColumn("TR", F.col("IPFTR15X") + F.col("IPDTR15X"))
    .withColumn("OF", F.col("IPFOF15X") + F.col("IPDOF15X"))
    .withColumn("SL", F.col("IPFSL15X") + F.col("IPDSL15X"))
    .withColumn("WC", F.col("IPFWC15X") + F.col("IPDWC15X"))
    .withColumn("OR", F.col("IPFOR15X") + F.col("IPDOR15X"))
    .withColumn("OU", F.col("IPFOU15X") + F.col("IPDOU15X"))
    .withColumn("OT", F.col("IPFOT15X") + F.col("IPDOT15X"))
    .withColumn("TOTEXP", F.col("IPXP15X"))
    .withColumn("EVNTYP", F.lit("IPAT"))
    .filter(F.col("TOTEXP") >= 0)
    .select(sop_cols)
)

hvis = (
    load_sas7bdat(spark, "C:/MEPS/h178h.sas7bdat")
    .withColumn("SF", F.col("HHSF15X")).withColumn("MR", F.col("HHMR15X"))
    .withColumn("MD", F.col("HHMD15X")).withColumn("PV", F.col("HHPV15X"))
    .withColumn("VA", F.col("HHVA15X")).withColumn("TR", F.col("HHTR15X"))
    .withColumn("OF", F.col("HHOF15X")).withColumn("SL", F.col("HHSL15X"))
    .withColumn("WC", F.col("HHWC15X")).withColumn("OR", F.col("HHOR15X"))
    .withColumn("OU", F.col("HHOU15X")).withColumn("OT", F.col("HHOT15X"))
    .withColumn("TOTEXP", F.col("HHXP15X"))
    .withColumn("EVNTYP", F.lit("HVIS"))
    .filter(F.col("TOTEXP") >= 0)
    .select(sop_cols)
)

opat = (
    load_sas7bdat(spark, "C:/MEPS/h178f.sas7bdat")
    .withColumn("SF", F.col("OPFSF15X") + F.col("OPDSF15X"))
    .withColumn("MR", F.col("OPFMR15X") + F.col("OPDMR15X"))
    .withColumn("MD", F.col("OPFMD15X") + F.col("OPDMD15X"))
    .withColumn("PV", F.col("OPFPV15X") + F.col("OPDPV15X"))
    .withColumn("VA", F.col("OPFVA15X") + F.col("OPDVA15X"))
    .withColumn("TR", F.col("OPFTR15X") + F.col("OPDTR15X"))
    .withColumn("OF", F.col("OPFOF15X") + F.col("OPDOF15X"))
    .withColumn("SL", F.col("OPFSL15X") + F.col("OPDSL15X"))
    .withColumn("WC", F.col("OPFWC15X") + F.col("OPDWC15X"))
    .withColumn("OR", F.col("OPFOR15X") + F.col("OPDOR15X"))
    .withColumn("OU", F.col("OPFOU15X") + F.col("OPDOU15X"))
    .withColumn("OT", F.col("OPFOT15X") + F.col("OPDOT15X"))
    .withColumn("TOTEXP", F.col("OPXP15X"))
    .withColumn("EVNTYP", F.lit("AMBU"))
    .filter(F.col("TOTEXP") >= 0)
    .select(sop_cols)
)

# 6) Combine all events
allevent = (
    ob.unionByName(erom).unionByName(ipat)
    .unionByName(hvis).unionByName(opat).unionByName(pmed3.select(sop_cols))
)

# 7) Subset events to those with diabetes
diab4 = diab3.join(allevent, on="EVNTIDX", how="inner")

# 8) Calculate estimates - all types of service combined
exp_cols = ["TOTEXP", "SF", "MR", "MD", "PV", "VA", "TR", "OF", "SL", "WC", "OR", "OU", "OT"]
all_pers = diab4.groupBy("DUPERSID").agg(
    *[F.sum(c).alias(c) for c in exp_cols]
)

h181 = load_sas7bdat(spark, "C:/MEPS/h181.sas7bdat")
fy1 = (
    h181.select("DUPERSID", "VARPSU", "VARSTR", "PERWT15F")
    .join(all_pers, on="DUPERSID", how="left")
    .withColumn("SUB",
        F.when(F.col("TOTEXP").isNotNull(), 1).otherwise(2))
    .fillna({c: 0 for c in exp_cols})
    .filter(F.col("PERWT15F") > 0)
)

print("=== Estimates for all events associated with diabetes, 2015 ===")
results = survey_mean(
    fy1,
    var_cols=exp_cols,
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
    domain_col="SUB",
)
print(results.to_string())

# 9) Estimates by type of service
tos = diab4.groupBy("DUPERSID", "EVNTYP").agg(
    F.count("*").alias("N_VISITS"),
    *[F.sum(c).alias(c) for c in exp_cols]
)

print("\n=== Estimates by type of service ===")
for evntyp in ["AMBU", "EROM", "IPAT", "HVIS", "PMED"]:
    tos_type = tos.filter(F.col("EVNTYP") == evntyp).drop("EVNTYP")
    fytos = (
        h181.select("DUPERSID", "VARPSU", "VARSTR", "PERWT15F")
        .join(tos_type, on="DUPERSID", how="left")
        .withColumn("SUB",
            F.when(F.col("TOTEXP").isNotNull(), 1).otherwise(2))
        .fillna({"N_VISITS": 0, **{c: 0 for c in exp_cols}})
        .filter(F.col("PERWT15F") > 0)
    )
    print(f"\n--- {evntyp} ---")
    res = survey_mean(
        fytos,
        var_cols=["N_VISITS"] + exp_cols,
        stratum_col="VARSTR",
        cluster_col="VARPSU",
        weight_col="PERWT15F",
        domain_col="SUB",
    )
    print(res.to_string())

spark.stop()
