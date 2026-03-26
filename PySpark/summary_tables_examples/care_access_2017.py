"""
Accessibility and quality of care: Access to Care, 2017

Reasons for difficulty receiving needed care
 - Number/percent of people
 - By poverty status

Input file: C:/MEPS/h201.sas7bdat (2017 full-year consolidated)

Replaces: SAS/summary_tables_examples/care_access_2017.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total
from utils.format_mappings import POVERTY_FORMAT

# Initialize Spark
spark = get_spark()

# Load FYC file
h201 = load_sas7bdat(spark, "C:/MEPS/h201.sas7bdat")

# Define variables
meps = (
    h201
    # Reasons for difficulty receiving needed care
    # any delay / unable to receive needed care
    .withColumn("delay_MD", F.when(
        (F.col("MDUNAB42") == 1) | (F.col("MDDLAY42") == 1), 1
    ).otherwise(0))
    .withColumn("delay_DN", F.when(
        (F.col("DNUNAB42") == 1) | (F.col("DNDLAY42") == 1), 1
    ).otherwise(0))
    .withColumn("delay_PM", F.when(
        (F.col("PMUNAB42") == 1) | (F.col("PMDLAY42") == 1), 1
    ).otherwise(0))

    # Among people unable or delayed, how many couldn't afford
    .withColumn("afford_MD", F.when(
        (F.col("MDDLRS42") == 1) | (F.col("MDUNRS42") == 1), 1
    ).otherwise(0))
    .withColumn("afford_DN", F.when(
        (F.col("DNDLRS42") == 1) | (F.col("DNUNRS42") == 1), 1
    ).otherwise(0))
    .withColumn("afford_PM", F.when(
        (F.col("PMDLRS42") == 1) | (F.col("PMUNRS42") == 1), 1
    ).otherwise(0))

    # ...had insurance problems
    .withColumn("insure_MD", F.when(
        F.col("MDDLRS42").isin(2, 3) | F.col("MDUNRS42").isin(2, 3), 1
    ).otherwise(0))
    .withColumn("insure_DN", F.when(
        F.col("DNDLRS42").isin(2, 3) | F.col("DNUNRS42").isin(2, 3), 1
    ).otherwise(0))
    .withColumn("insure_PM", F.when(
        F.col("PMDLRS42").isin(2, 3) | F.col("PMUNRS42").isin(2, 3), 1
    ).otherwise(0))

    # ...other
    .withColumn("other_MD", F.when(
        (F.col("MDDLRS42") > 3) | (F.col("MDUNRS42") > 3), 1
    ).otherwise(0))
    .withColumn("other_DN", F.when(
        (F.col("DNDLRS42") > 3) | (F.col("DNUNRS42") > 3), 1
    ).otherwise(0))
    .withColumn("other_PM", F.when(
        (F.col("PMDLRS42") > 3) | (F.col("PMUNRS42") > 3), 1
    ).otherwise(0))
)

# Combined ANY variables
meps = (
    meps
    .withColumn("delay_ANY", F.when(
        (F.col("delay_MD") == 1) | (F.col("delay_DN") == 1) | (F.col("delay_PM") == 1), 1
    ).otherwise(0))
    .withColumn("afford_ANY", F.when(
        (F.col("afford_MD") == 1) | (F.col("afford_DN") == 1) | (F.col("afford_PM") == 1), 1
    ).otherwise(0))
    .withColumn("insure_ANY", F.when(
        (F.col("insure_MD") == 1) | (F.col("insure_DN") == 1) | (F.col("insure_PM") == 1), 1
    ).otherwise(0))
    .withColumn("other_ANY", F.when(
        (F.col("other_MD") == 1) | (F.col("other_DN") == 1) | (F.col("other_PM") == 1), 1
    ).otherwise(0))
)

# Define domain: persons eligible for access to care supplement who had difficulty
meps = (
    meps
    .withColumn("domain", F.when(
        (F.col("ACCELI42") == 1) & (F.col("delay_ANY") == 1), 1
    ).otherwise(0))
    # Adjust weights so observations are not dropped
    .withColumn("PERWT17F", F.when(
        (F.col("domain") == 0) & (F.col("PERWT17F") == 0), 1
    ).otherwise(F.col("PERWT17F")))
)

# QC new variables
print("=== QC: delay variables ===")
meps.groupBy("delay_MD", "MDUNAB42", "MDDLAY42").count().show()
meps.groupBy("delay_ANY", "delay_MD", "delay_DN", "delay_PM").count().show()

# Calculate estimates using survey procedures
# Reasons for difficulty receiving any needed care, by poverty status
#   sum  = Number of people
#   mean = Percent of people

# Apply poverty format label
meps = meps.withColumn(
    "poverty_cat",
    F.when(F.col("POVCAT17") == 1, "1 Negative or poor")
    .when(F.col("POVCAT17") == 2, "2 Near-poor")
    .when(F.col("POVCAT17") == 3, "3 Low income")
    .when(F.col("POVCAT17") == 4, "4 Middle Income")
    .when(F.col("POVCAT17") == 5, "5 High Income")
)

# Filter to domain and estimate by poverty status
domain_df = meps.filter(F.col("domain") == 1)

print("=== Survey-weighted means: Reasons for difficulty, by poverty status ===")
results_mean = survey_mean(
    meps,
    var_cols=["afford_ANY", "insure_ANY", "other_ANY"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT17F",
    domain_col="poverty_cat",
)
print(results_mean.to_string())

print("\n=== Survey-weighted totals: Reasons for difficulty, by poverty status ===")
results_total = survey_total(
    meps,
    var_cols=["afford_ANY", "insure_ANY", "other_ANY"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT17F",
    domain_col="poverty_cat",
)
print(results_total.to_string())

spark.stop()
