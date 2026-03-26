"""
Prescribed Medicines, 2016:
 - Total purchases and expenditures by generic drug name (RXDRGNAM)
 - Number of people with purchase

Input file: C:/MEPS/h188a.ssp (2016 RX event file)
            C:/MEPS/h192.ssp (2016 full-year consolidated)

Replaces: SAS/summary_tables_examples/pmed_prescribed_drug_2016.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_ssp
from utils.survey_utils import survey_mean, survey_total

# Initialize Spark
spark = get_spark()

# Load event files
h188a = load_ssp(spark, "C:/MEPS/h188a.ssp")  # RX events
h192 = load_ssp(spark, "C:/MEPS/h192.ssp")    # FYC

# Aggregate to person-level, by drug name
pers_drug = (
    h188a
    .groupBy("DUPERSID", "RXDRGNAM")
    .agg(
        F.sum("RXXP16X").alias("pers_XP"),
        F.count("*").alias("n_purchases"),
    )
    .withColumn("person", F.lit(1))
)

# Merge with FYC for survey design variables
fyc_vars = h192.select("DUPERSID", "VARSTR", "VARPSU", "PERWT16F")
pers_drug = pers_drug.join(fyc_vars, on="DUPERSID", how="inner")

# Calculate estimates using survey procedures
print("=== Total purchases by drug name (top 20) ===")
results_purchases = survey_total(
    pers_drug,
    var_cols=["n_purchases"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
    domain_col="RXDRGNAM",
)
print(results_purchases.to_string())

print("\n=== Total expenditures by drug name (top 20) ===")
results_exp = survey_total(
    pers_drug,
    var_cols=["pers_XP"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
    domain_col="RXDRGNAM",
)
print(results_exp.to_string())

print("\n=== Number of people with purchase, by drug name ===")
results_person = survey_total(
    pers_drug,
    var_cols=["person"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
    domain_col="RXDRGNAM",
)
print(results_person.to_string())

spark.stop()
