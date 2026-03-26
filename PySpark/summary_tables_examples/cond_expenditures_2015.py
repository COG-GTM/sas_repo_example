"""
Medical Conditions, 2015:
 - Number of people with care
 - Number of events
 - Total expenditures
 - Mean expenditure per person

Note: For 2015, conditions use ICD-9 and CCS codes (pre-2016)

Input files:
    - C:/MEPS/h178a.ssp (2015 RX event file)
    - C:/MEPS/h178d.ssp (2015 IP event file)
    - C:/MEPS/h178e.ssp (2015 ER event file)
    - C:/MEPS/h178f.ssp (2015 OP event file)
    - C:/MEPS/h178g.ssp (2015 OB event file)
    - C:/MEPS/h178h.ssp (2015 HH event file)
    - C:/MEPS/h178if1.ssp (2015 CLNK: Condition-event link file)
    - C:/MEPS/h180.ssp (2015 Conditions file)

Replaces: SAS/summary_tables_examples/cond_expenditures_2015.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from utils.data_loader import get_spark, load_ssp
from utils.survey_utils import survey_mean, survey_total

# CCS code to collapsed condition mapping (replaces SAS PROC FORMAT CCCFMT)
CCS_FORMAT = {
    range(1, 10): "Infectious diseases",
    range(11, 46): "Cancer",
    (46, 47): "Non-malignant neoplasm",
    (48,): "Thyroid disease",
    (49, 50): "Diabetes mellitus",
    (51, 52, 54, 55, 56, 57, 58): "Other endocrine, nutritional & immune disorder",
    (53,): "Hyperlipidemia",
    (59,): "Anemia and other deficiencies",
    range(60, 65): "Hemorrhagic, coagulation, and disorders of White Blood cells",
    tuple(range(65, 76)) + tuple(range(650, 671)): "Mental disorders",
    range(76, 79): "CNS infection",
    range(79, 82): "Hereditary, degenerative and other nervous system disorders",
    (82,): "Paralysis",
    (84,): "Headache",
    (83,): "Epilepsy and convulsions",
    (85,): "Coma, brain damage",
    (86,): "Cataract",
    (88,): "Glaucoma",
    (87, 89, 90, 91): "Other eye disorders",
    (92,): "Otitis media",
    range(93, 96): "Other CNS disorders",
    (98, 99): "Hypertension",
    (96, 97) + tuple(range(100, 109)): "Heart disease",
    range(109, 114): "Cerebrovascular disease",
    range(114, 122): "Other circulatory conditions arteries, veins, and lymphatics",
    (122,): "Pneumonia",
    (123,): "Influenza",
    (124,): "Tonsillitis",
    (125, 126): "Acute Bronchitis and URI",
    range(127, 135): "COPD, asthma",
    (135,): "Intestinal infection",
    (136,): "Disorders of teeth and jaws",
    (137,): "Disorders of mouth and esophagus",
    range(138, 142): "Disorders of the upper GI",
    (142,): "Appendicitis",
    (143,): "Hernias",
    range(144, 149): "Other stomach and intestinal disorders",
    range(153, 156): "Other GI",
    range(149, 153): "Gallbladder, pancreatic, and liver disease",
    (156, 157, 158, 160, 161): "Kidney Disease",
    (159,): "Urinary tract infections",
    (162, 163): "Other urinary",
    range(164, 167): "Male genital disorders",
    (167,): "Non-malignant breast disease",
    range(168, 177): "Female genital disorders, and contraception",
    range(177, 196): "Complications of pregnancy and birth",
    (196, 218): "Normal birth/live born",
    range(197, 201): "Skin disorders",
    range(201, 205): "Osteoarthritis and other non-traumatic joint disorders",
    (205,): "Back problems",
    (206, 207, 208, 209, 212): "Other bone and musculoskeletal disease",
    (210, 211): "Systemic lupus and connective tissues disorders",
    range(213, 218): "Congenital anomalies",
    range(219, 225): "Perinatal Conditions",
    tuple(range(225, 237)) + (239, 240, 244): "Trauma-related disorders",
    (237, 238): "Complications of surgery or device",
    range(241, 244): "Poisoning by medical and non-medical substances",
    (259,): "Residual Codes",
    (10,) + tuple(range(254, 259)): "Other care and screening",
    range(245, 253): "Symptoms",
    (253,): "Allergic reactions",
}


def map_ccs_to_condition(ccs_code):
    """Map a CCS code to a collapsed condition category."""
    if ccs_code is None or ccs_code < 0:
        return None
    for key, condition in CCS_FORMAT.items():
        if isinstance(key, range):
            if ccs_code in key:
                return condition
        elif isinstance(key, tuple):
            if ccs_code in key:
                return condition
    return "Other"


# Register UDF
from pyspark.sql.types import StringType  # noqa: E402

# Initialize Spark
spark = get_spark()
map_ccs_udf = F.udf(map_ccs_to_condition, StringType())

# Load datasets
h178a = load_ssp(spark, "C:/MEPS/h178a.ssp")  # RX
h178d = load_ssp(spark, "C:/MEPS/h178d.ssp")  # IP
h178e = load_ssp(spark, "C:/MEPS/h178e.ssp")  # ER
h178f = load_ssp(spark, "C:/MEPS/h178f.ssp")  # OP
h178g = load_ssp(spark, "C:/MEPS/h178g.ssp")  # OB
h178h = load_ssp(spark, "C:/MEPS/h178h.ssp")  # HH
h178if1 = load_ssp(spark, "C:/MEPS/h178if1.ssp")  # CLNK
h180 = load_ssp(spark, "C:/MEPS/h180.ssp")  # Conditions

# For RX events, count number of fills per event
rx_pers = (
    h178a
    .groupBy("DUPERSID", "LINKIDX", "VARSTR", "VARPSU", "PERWT15F")
    .agg(
        F.sum("RXXP15X").alias("XP15X"),
        F.count("*").alias("n_fills"),
    )
    .withColumnRenamed("LINKIDX", "EVNTIDX")
    .withColumn("data", F.lit("RX"))
)

# Prepare other event files with renamed expenditure columns
ip = h178d.withColumnRenamed("IPXP15X", "XP15X").withColumn("data", F.lit("IP"))
er = h178e.withColumnRenamed("ERXP15X", "XP15X").withColumn("data", F.lit("ER"))
op = h178f.withColumnRenamed("OPXP15X", "XP15X").withColumn("data", F.lit("OP"))
ob = h178g.withColumnRenamed("OBXP15X", "XP15X").withColumn("data", F.lit("OB"))
hh = h178h.withColumnRenamed("HHXP15X", "XP15X").withColumn("data", F.lit("HH"))

# Add n_fills = 1 for non-RX events
for evt_df_name in ["ip", "er", "op", "ob", "hh"]:
    locals()[evt_df_name] = locals()[evt_df_name].withColumn("n_fills", F.lit(1))

# Stack event files
common_cols = ["data", "EVNTIDX", "DUPERSID", "XP15X", "VARSTR", "VARPSU", "PERWT15F", "n_fills"]
stacked_events = (
    rx_pers.select(common_cols)
    .unionByName(ip.select(common_cols), allowMissingColumns=True)
    .unionByName(er.select(common_cols), allowMissingColumns=True)
    .unionByName(op.select(common_cols), allowMissingColumns=True)
    .unionByName(ob.select(common_cols), allowMissingColumns=True)
    .unionByName(hh.select(common_cols), allowMissingColumns=True)
)

# n_events = max(n_fills, 1)
stacked_events = stacked_events.withColumn(
    "n_events", F.greatest(F.col("n_fills"), F.lit(1))
)

# Merge conditions with CLNK file
clnk = h178if1.select("DUPERSID", "CONDIDX", "EVNTIDX")
cond = h180.select("DUPERSID", "CONDIDX", "CCCODEX")

cond_clink = clnk.join(cond, on=["DUPERSID", "CONDIDX"], how="inner")

# Map CCS code to collapsed condition
cond_clink = cond_clink.withColumn(
    "Condition", map_ccs_udf(F.col("CCCODEX").cast("int"))
)

# De-duplicate by event ID and collapsed condition
cond_clink = cond_clink.dropDuplicates(["DUPERSID", "EVNTIDX", "Condition"])

# Merge events with linked conditions
all_events = stacked_events.join(
    cond_clink.select("DUPERSID", "EVNTIDX", "Condition"),
    on=["DUPERSID", "EVNTIDX"],
    how="inner",
)

# Remove missing conditions and negative expenditures
all_events = all_events.filter(
    (F.col("Condition").isNotNull()) & (F.col("Condition") != "")
    & (F.col("XP15X") >= 0)
)

# Aggregate to person-level, by Condition
all_pers = (
    all_events
    .groupBy("DUPERSID", "VARSTR", "VARPSU", "Condition")
    .agg(
        F.mean("XP15X").alias("mean_XP"),
        F.sum("XP15X").alias("pers_XP"),
        F.sum("n_events").alias("n_events"),
        F.first("PERWT15F").alias("PERWT15F"),
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
    weight_col="PERWT15F",
    domain_col="Condition",
)
print(results_person.to_string())

print("\n=== Number of events, by condition ===")
results_events = survey_total(
    all_pers,
    var_cols=["n_events"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
    domain_col="Condition",
)
print(results_events.to_string())

print("\n=== Total expenditures and mean per person, by condition ===")
results_exp_total = survey_total(
    all_pers,
    var_cols=["pers_XP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
    domain_col="Condition",
)
print(results_exp_total.to_string())

results_exp_mean = survey_mean(
    all_pers,
    var_cols=["pers_XP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
    domain_col="Condition",
)
print(results_exp_mean.to_string())

spark.stop()
