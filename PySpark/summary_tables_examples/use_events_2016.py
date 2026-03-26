"""
Utilization and Expenditures, 2016:
 - Total number of events by event type

Input files:
    - C:/MEPS/h188a.ssp (2016 RX event file)
    - C:/MEPS/h188d.ssp (2016 IP event file)
    - C:/MEPS/h188e.ssp (2016 ER event file)
    - C:/MEPS/h188f.ssp (2016 OP event file)
    - C:/MEPS/h188g.ssp (2016 OB event file)
    - C:/MEPS/h188h.ssp (2016 HH event file)
    - C:/MEPS/h192.ssp (2016 full-year consolidated)

Replaces: SAS/summary_tables_examples/use_events_2016.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_ssp
from utils.survey_utils import survey_mean, survey_total

# Initialize Spark
spark = get_spark()

# Load FYC file for survey design variables and utilization counts
h192 = load_ssp(spark, "C:/MEPS/h192.ssp")

# Select utilization variables from FYC
# These are person-level event counts already in the FYC
meps = h192.select(
    "DUPERSID", "VARSTR", "VARPSU", "PERWT16F",
    "OBTOTV16",  # Office-based visits
    "OPTOTV16",  # Outpatient visits
    "ERTOT16",   # ER visits
    "IPDIS16",   # Inpatient discharges
    "HHTOTD16",  # Home health days
    "RXTOT16",   # RX fills
)

# Calculate estimates using survey procedures
print("=== Total number of events by type, 2016 ===")
var_cols = ["OBTOTV16", "OPTOTV16", "ERTOT16", "IPDIS16", "HHTOTD16", "RXTOT16"]

results = survey_total(
    meps,
    var_cols=var_cols,
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
)
print(results.to_string())

print("\n=== Mean number of events per person by type, 2016 ===")
results_mean = survey_mean(
    meps,
    var_cols=var_cols,
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
)
print(results_mean.to_string())

spark.stop()
