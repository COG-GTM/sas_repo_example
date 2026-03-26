"""
Accessibility and quality of care: Access to Care, 2019

Did not receive treatment because couldn't afford it
 - Number/percent of people
 - By poverty status

Input file: C:/MEPS/h216.sas7bdat (2019 full-year consolidated)

Replaces: SAS/summary_tables_examples/care_access_2019.sas
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
h216 = load_sas7bdat(spark, "C:/MEPS/h216.sas7bdat")

# Define variables
meps = (
    h216
    # Didn't receive care because couldn't afford it
    .withColumn("afford_MD", F.when(F.col("AFRDCA42") == 1, 1).otherwise(0))
    .withColumn("afford_DN", F.when(F.col("AFRDDN42") == 1, 1).otherwise(0))
    .withColumn("afford_PM", F.when(F.col("AFRDPM42") == 1, 1).otherwise(0))
    .withColumn("afford_ANY", F.when(
        (F.col("AFRDCA42") == 1) | (F.col("AFRDDN42") == 1) | (F.col("AFRDPM42") == 1), 1
    ).otherwise(0))

    # Define domain: persons eligible for access to care supplement
    .withColumn("domain", F.when(F.col("ACCELI42") == 1, 1).otherwise(0))

    # Adjust weights so SAS doesn't drop observations
    .withColumn("PERWT19F", F.when(
        (F.col("domain") == 0) & (F.col("PERWT19F") == 0), 1
    ).otherwise(F.col("PERWT19F")))
)

# Apply poverty format label
meps = meps.withColumn(
    "poverty_cat",
    F.when(F.col("POVCAT19") == 1, "1 Negative or poor")
    .when(F.col("POVCAT19") == 2, "2 Near-poor")
    .when(F.col("POVCAT19") == 3, "3 Low income")
    .when(F.col("POVCAT19") == 4, "4 Middle Income")
    .when(F.col("POVCAT19") == 5, "5 High Income")
)

# QC new variables
print("=== QC: afford variables ===")
meps.groupBy("AFRDCA42", "afford_MD").count().show()
meps.groupBy("afford_MD", "afford_DN", "afford_PM", "afford_ANY").count().show()

# Calculate estimates using survey procedures
# Did not receive treatment because couldn't afford it, by poverty status
#   sum  = Number of people
#   mean = Percent of people

print("=== Survey-weighted means: Afford care, by poverty status ===")
results_mean = survey_mean(
    meps,
    var_cols=["afford_ANY", "afford_MD", "afford_DN", "afford_PM"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT19F",
    domain_col="poverty_cat",
)
print(results_mean.to_string())

print("\n=== Survey-weighted totals: Afford care, by poverty status ===")
results_total = survey_total(
    meps,
    var_cols=["afford_ANY", "afford_MD", "afford_DN", "afford_PM"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT19F",
    domain_col="poverty_cat",
)
print(results_total.to_string())

spark.stop()
