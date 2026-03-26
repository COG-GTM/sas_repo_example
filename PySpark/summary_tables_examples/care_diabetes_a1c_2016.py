"""
Accessibility and quality of care: Diabetes Care, 2016

Diabetes care survey (DCS):
 - Number/percent of adults with diabetes receiving hemoglobin A1c blood test
 - By race/ethnicity

Input file: C:/MEPS/h192.ssp (2016 full-year consolidated)

Replaces: SAS/summary_tables_examples/care_diabetes_a1c_2016.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_ssp
from utils.survey_utils import survey_freq

# Initialize Spark
spark = get_spark()

# Load FYC file (.ssp XPORT format for 2016)
h192 = load_ssp(spark, "C:/MEPS/h192.ssp")

# Define variables
meps = (
    h192
    # Define domain and adjust weights
    .withColumn("domain", F.when(F.col("DIABW16F") > 0, 1).otherwise(0))
    .withColumn("DIABW16F", F.when(
        F.col("domain") == 0, 1
    ).otherwise(F.col("DIABW16F")))

    # Race/ethnicity (for 2012 and later, use RACETHX and RACEV1X)
    .withColumn("hisp", F.when(F.col("RACETHX") == 1, 1).otherwise(0))
    .withColumn("white", F.when(F.col("RACETHX") == 2, 1).otherwise(0))
    .withColumn("black", F.when(F.col("RACETHX") == 3, 1).otherwise(0))
    .withColumn("native", F.when(
        (F.col("RACETHX") > 3) & F.col("RACEV1X").isin(3, 6), 1
    ).otherwise(0))
    .withColumn("asian", F.when(
        (F.col("RACETHX") > 3) & F.col("RACEV1X").isin(4, 5), 1
    ).otherwise(0))
    .withColumn("race", (
        1 * F.col("hisp") + 2 * F.col("white") + 3 * F.col("black")
        + 4 * F.col("native") + 5 * F.col("asian")
    ))
)

# Apply format labels
meps = (
    meps
    .withColumn("race_label",
        F.when(F.col("race") == 1, "1 Hispanic")
        .when(F.col("race") == 2, "2 White")
        .when(F.col("race") == 3, "3 Black")
        .when(F.col("race") == 4, "4 Amer. Indian, AK Native, or mult. races")
        .when(F.col("race") == 5, "5 Asian, Hawaiian, or Pacific Islander")
        .otherwise("Missing"))
    .withColumn("a1c_label",
        F.when(F.col("DSA1C53").isin(-9, -8, -7), "Don't know/Non-response")
        .when(F.col("DSA1C53") == -1, "Inapplicable")
        .when(F.col("DSA1C53").isin(0, 96), "Did not have measurement")
        .when((F.col("DSA1C53") >= 1) & (F.col("DSA1C53") <= 95), "Had measurement")
        .otherwise("Missing"))
)

# QC new variables
print("=== QC: race variables ===")
meps.groupBy("RACETHX", "race").count().orderBy("RACETHX").show()

# Calculate estimates using survey procedures
# Use DIABW16F weight variable, since outcome variable comes from DCS
print("=== Survey-weighted frequencies: A1C measurement by race ===")
domain_df = meps.filter(F.col("domain") == 1)

results = survey_freq(
    domain_df,
    table_vars=["a1c_label", "race_label"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="DIABW16F",
)
print(results.to_string())

# Print summary table
print("\n=== Adults with diabetes with hemoglobin A1C measurement in 2016, by race ===")
domain_df.groupBy("a1c_label", "race_label").count().orderBy("a1c_label", "race_label").show(50)

spark.stop()
