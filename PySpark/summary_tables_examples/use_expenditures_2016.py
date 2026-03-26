"""
Utilization and Expenditures, 2016:
 - Total expenditures by event type and source of payment

Input file: C:/MEPS/h192.ssp (2016 full-year consolidated)

Replaces: SAS/summary_tables_examples/use_expenditures_2016.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_ssp
from utils.survey_utils import survey_mean, survey_total

# Initialize Spark
spark = get_spark()

# Load FYC file
h192 = load_ssp(spark, "C:/MEPS/h192.ssp")

# Expenditure variables by type of service
# Total expenditures
exp_vars_total = [
    "TOTEXP16",   # Total
    "OBVEXP16",   # Office-based visits
    "OPTEXP16",   # Outpatient
    "ERTEXP16",   # Emergency room
    "IPTEXP16",   # Inpatient
    "HHAEXP16",   # Home health agency
    "HHNEXP16",   # Home health non-agency
    "VISEXP16",   # Vision
    "RXEXP16",    # Prescribed medicines
    "DVTEXP16",   # Dental visits
    "OTHEXP16",   # Other
]

# Source of payment variables (for total expenditures)
sop_vars = [
    "TOTSLF16",  # Self/family
    "TOTMCR16",  # Medicare
    "TOTMCD16",  # Medicaid
    "TOTPRV16",  # Private insurance
    "TOTVA16",   # VA
    "TOTTRI16",  # TRICARE
    "TOTOFD16",  # Other federal
    "TOTSTL16",  # Other state/local
    "TOTWCP16",  # Workers' comp
    "TOTOPR16",  # Other private
    "TOTOPU16",  # Other public
    "TOTOSR16",  # Other sources
]

meps = h192.select(
    ["DUPERSID", "VARSTR", "VARPSU", "PERWT16F"] + exp_vars_total + sop_vars
)

# Calculate estimates
print("=== Total expenditures by type of service, 2016 ===")
results_type = survey_total(
    meps,
    var_cols=exp_vars_total,
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
)
print(results_type.to_string())

print("\n=== Mean expenditure per person by type of service, 2016 ===")
results_mean = survey_mean(
    meps,
    var_cols=exp_vars_total,
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
)
print(results_mean.to_string())

print("\n=== Total expenditures by source of payment, 2016 ===")
results_sop = survey_total(
    meps,
    var_cols=sop_vars,
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
)
print(results_sop.to_string())

spark.stop()
