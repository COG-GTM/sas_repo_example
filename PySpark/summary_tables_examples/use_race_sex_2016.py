"""
Utilization and Expenditures, 2016:
 - Mean expenditure per person by race/ethnicity and sex

Input file: C:/MEPS/h192.ssp (2016 full-year consolidated)

Replaces: SAS/summary_tables_examples/use_race_sex_2016.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_ssp
from utils.survey_utils import survey_mean
from utils.format_mappings import RACE_FORMAT, SEX_FORMAT

# Initialize Spark
spark = get_spark()

# Load FYC file
h192 = load_ssp(spark, "C:/MEPS/h192.ssp")

# Define variables
meps = (
    h192
    .withColumn("race_label",
        F.when(F.col("RACETHX") == 1, "Hispanic")
        .when(F.col("RACETHX") == 2, "NH White only")
        .when(F.col("RACETHX") == 3, "NH Black only")
        .when(F.col("RACETHX") == 4, "NH Asian only")
        .when(F.col("RACETHX") == 5, "NH Other Race Including Multiple Races"))
    .withColumn("sex_label",
        F.when(F.col("SEX") == 1, "Male")
        .when(F.col("SEX") == 2, "Female"))
)

# Expenditure variables
exp_vars = ["TOTEXP16", "OBVEXP16", "RXEXP16", "IPTEXP16", "ERTEXP16"]

# Calculate estimates by race
print("=== Mean expenditure per person by race/ethnicity, 2016 ===")
results_race = survey_mean(
    meps,
    var_cols=exp_vars,
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
    domain_col="race_label",
)
print(results_race.to_string())

# Calculate estimates by sex
print("\n=== Mean expenditure per person by sex, 2016 ===")
results_sex = survey_mean(
    meps,
    var_cols=exp_vars,
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT16F",
    domain_col="sex_label",
)
print(results_sex.to_string())

spark.stop()
