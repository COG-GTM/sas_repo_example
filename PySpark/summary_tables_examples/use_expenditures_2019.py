"""
Utilization and Expenditures, 2019:
 - Total expenditures by event type and source of payment

Input file: C:/MEPS/h216.sas7bdat (2019 full-year consolidated)

Replaces: SAS/summary_tables_examples/use_expenditures_2019.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total

# Initialize Spark
spark = get_spark()

# Load FYC file
h216 = load_sas7bdat(spark, "C:/MEPS/h216.sas7bdat")

# Expenditure variables by type of service
exp_vars_total = [
    "TOTEXP19",   # Total
    "OBVEXP19",   # Office-based visits
    "OPTEXP19",   # Outpatient
    "ERTEXP19",   # Emergency room
    "IPTEXP19",   # Inpatient
    "HHAEXP19",   # Home health agency
    "HHNEXP19",   # Home health non-agency
    "VISEXP19",   # Vision
    "RXEXP19",    # Prescribed medicines
    "DVTEXP19",   # Dental visits
    "OTHEXP19",   # Other
]

# Source of payment variables
sop_vars = [
    "TOTSLF19",  # Self/family
    "TOTMCR19",  # Medicare
    "TOTMCD19",  # Medicaid
    "TOTPRV19",  # Private insurance
    "TOTVA19",   # VA
    "TOTTRI19",  # TRICARE
    "TOTOFD19",  # Other federal
    "TOTSTL19",  # Other state/local
    "TOTWCP19",  # Workers' comp
    "TOTOPR19",  # Other private
    "TOTOPU19",  # Other public
    "TOTOSR19",  # Other sources
]

meps = h216.select(
    ["DUPERSID", "VARSTR", "VARPSU", "PERWT19F"] + exp_vars_total + sop_vars
)

# Calculate estimates
print("=== Total expenditures by type of service, 2019 ===")
results_type = survey_total(
    meps,
    var_cols=exp_vars_total,
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT19F",
)
print(results_type.to_string())

print("\n=== Mean expenditure per person by type of service, 2019 ===")
results_mean = survey_mean(
    meps,
    var_cols=exp_vars_total,
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT19F",
)
print(results_mean.to_string())

print("\n=== Total expenditures by source of payment, 2019 ===")
results_sop = survey_total(
    meps,
    var_cols=sop_vars,
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT19F",
)
print(results_sop.to_string())

spark.stop()
