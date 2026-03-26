"""
Medical Conditions, 2018:
 - Number of people with care
 - Number of events
 - Total expenditures
 - Mean expenditure per person

Note: Starting in 2016, conditions use ICD-10 and CCSR codes

Input files:
    - C:/MEPS/h206a.sas7bdat (2018 RX event file)
    - C:/MEPS/h206d.sas7bdat (2018 IP event file)
    - C:/MEPS/h206e.sas7bdat (2018 ER event file)
    - C:/MEPS/h206f.sas7bdat (2018 OP event file)
    - C:/MEPS/h206g.sas7bdat (2018 OB event file)
    - C:/MEPS/h206h.sas7bdat (2018 HH event file)
    - C:/MEPS/h206if1.sas7bdat (2018 CLNK: Condition-event link file)
    - C:/MEPS/h207.sas7bdat (2018 Conditions file)

Replaces: SAS/summary_tables_examples/cond_expenditures_2018.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total

# Initialize Spark
spark = get_spark()

# Load event files
rx = load_sas7bdat(spark, "C:/MEPS/h206a.sas7bdat")
ip = load_sas7bdat(spark, "C:/MEPS/h206d.sas7bdat")
er = load_sas7bdat(spark, "C:/MEPS/h206e.sas7bdat")
op = load_sas7bdat(spark, "C:/MEPS/h206f.sas7bdat")
ob = load_sas7bdat(spark, "C:/MEPS/h206g.sas7bdat")
hh = load_sas7bdat(spark, "C:/MEPS/h206h.sas7bdat")

# Load CLNK and conditions files
clnk = load_sas7bdat(spark, "C:/MEPS/h206if1.sas7bdat")
cond_puf = load_sas7bdat(spark, "C:/MEPS/h207.sas7bdat")

# Load crosswalk for CCSR and collapsed conditions codes
ccsr_url = (
    "https://raw.githubusercontent.com/HHS-AHRQ/MEPS/master/"
    "Quick_Reference_Guides/meps_ccsr_conditions.csv"
)
condition_codes_pdf = pd.read_csv(ccsr_url)
condition_codes_pdf = condition_codes_pdf.rename(columns={
    "CCSR_Code": "CCSR",
    "MEPS_collapsed_condition_categor": "Condition",
})
condition_codes_pdf = condition_codes_pdf[["CCSR", "Condition"]]
condition_codes = spark.createDataFrame(condition_codes_pdf)

# For RX events, count number of fills per event
rx_pers = (
    rx
    .groupBy("DUPERSID", "LINKIDX", "VARSTR", "VARPSU", "PERWT18F")
    .agg(
        F.sum("RXXP18X").alias("XP18X"),
        F.count("*").alias("n_fills"),
    )
    .withColumnRenamed("LINKIDX", "EVNTIDX")
    .withColumn("data", F.lit("RX"))
)

# Prepare other event files
ip = ip.withColumnRenamed("IPXP18X", "XP18X").withColumn("data", F.lit("IP")).withColumn("n_fills", F.lit(1))
er = er.withColumnRenamed("ERXP18X", "XP18X").withColumn("data", F.lit("ER")).withColumn("n_fills", F.lit(1))
op = op.withColumnRenamed("OPXP18X", "XP18X").withColumn("data", F.lit("OP")).withColumn("n_fills", F.lit(1))
ob = ob.withColumnRenamed("OBXP18X", "XP18X").withColumn("data", F.lit("OB")).withColumn("n_fills", F.lit(1))
hh = hh.withColumnRenamed("HHXP18X", "XP18X").withColumn("data", F.lit("HH")).withColumn("n_fills", F.lit(1))

# Stack event files
common_cols = ["data", "EVNTIDX", "DUPERSID", "XP18X", "VARSTR", "VARPSU", "PERWT18F", "n_fills"]
stacked_events = (
    rx_pers.select(common_cols)
    .unionByName(ip.select(common_cols), allowMissingColumns=True)
    .unionByName(er.select(common_cols), allowMissingColumns=True)
    .unionByName(op.select(common_cols), allowMissingColumns=True)
    .unionByName(ob.select(common_cols), allowMissingColumns=True)
    .unionByName(hh.select(common_cols), allowMissingColumns=True)
)
stacked_events = stacked_events.withColumn(
    "n_events", F.greatest(F.col("n_fills"), F.lit(1))
)

# Merge conditions file with the CLNK file
clnk_sub = clnk.select("DUPERSID", "CONDIDX", "EVNTIDX")

# Get all CCSR columns from conditions file
ccsr_cols = [c for c in cond_puf.columns if c.startswith("CCSR")]
cond_sub = cond_puf.select(["DUPERSID", "CONDIDX"] + ccsr_cols)

cond_clink = clnk_sub.join(cond_sub, on=["DUPERSID", "CONDIDX"], how="inner")

# Convert multiple CCSRs to separate lines (wide to long)
# Equivalent of SAS PROC TRANSPOSE
ccsr_array = F.array(*[F.col(c) for c in ccsr_cols])
cond_long = (
    cond_clink
    .withColumn("CCSR", F.explode(ccsr_array))
    .filter(F.col("CCSR") != "-1")
    .filter(F.col("CCSR").isNotNull())
    .filter(F.col("CCSR") != "")
    .select("DUPERSID", "CONDIDX", "EVNTIDX", "CCSR")
)

# Merge on collapsed condition codes
cond = cond_long.join(condition_codes, on="CCSR", how="left")
cond = cond.filter(
    (F.col("Condition").isNotNull()) & (F.col("Condition") != "")
)

# De-duplicate by event ID and collapsed condition
cond = cond.select("DUPERSID", "EVNTIDX", "Condition").dropDuplicates(
    ["DUPERSID", "EVNTIDX", "Condition"]
)

# Merge events with linked conditions
all_events = stacked_events.join(
    cond,
    on=["DUPERSID", "EVNTIDX"],
    how="inner",
)
all_events = all_events.filter(
    (F.col("Condition").isNotNull()) & (F.col("Condition") != "")
    & (F.col("XP18X") >= 0)
)

# Aggregate to person-level, by Condition
all_pers = (
    all_events
    .groupBy("DUPERSID", "VARSTR", "VARPSU", "Condition")
    .agg(
        F.mean("XP18X").alias("mean_XP"),
        F.sum("XP18X").alias("pers_XP"),
        F.sum("n_events").alias("n_events"),
        F.first("PERWT18F").alias("PERWT18F"),
    )
    .withColumn("person", F.lit(1))
)

# Calculate estimates using survey procedures
print("=== Number of people with care, by condition ===")
results_person = survey_total(
    all_pers,
    var_cols=["person"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT18F",
    domain_col="Condition",
)
print(results_person.to_string())

print("\n=== Number of events, by condition ===")
results_events = survey_total(
    all_pers,
    var_cols=["n_events"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT18F",
    domain_col="Condition",
)
print(results_events.to_string())

print("\n=== Total expenditures and mean per person, by condition ===")
results_exp_total = survey_total(
    all_pers,
    var_cols=["pers_XP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT18F",
    domain_col="Condition",
)
print(results_exp_total.to_string())

results_exp_mean = survey_mean(
    all_pers,
    var_cols=["pers_XP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT18F",
    domain_col="Condition",
)
print(results_exp_mean.to_string())

spark.stop()
