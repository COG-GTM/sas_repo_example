"""
Exercise 1b: National Health Care Expenses by Type of Service, 2015

Generates the following estimates:
  (1) Percentage distribution of expenses by type of service
  (2) Percentage of persons with an expense, by type of service
  (3) Mean expense per person with an expense, by type of service

Service categories:
  - Hospital Inpatient
  - Ambulatory (Office-Based & Hospital Outpatient)
  - Prescribed Medicines
  - Dental
  - Emergency Room
  - Home Health & Other

Input file: C:/MEPS/h181.sas7bdat (2015 Full-Year File)

Replaces: SAS/workshop_exercises/exercise_1b/Exercise1b.sas
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from utils.data_loader import get_spark, load_sas7bdat
from utils.survey_utils import survey_mean, survey_total

spark = get_spark()

# Load 2015 FYC
h181 = load_sas7bdat(spark, "C:/MEPS/h181.sas7bdat")

# Define expenditure variables by type of service
puf181 = (
    h181
    .withColumn("TOTAL", F.col("TOTEXP15"))
    .withColumn("HOSPITAL_INPATIENT", F.col("IPDEXP15") + F.col("IPFEXP15"))
    .withColumn("AMBULATORY",
        F.col("OBVEXP15") + F.col("OPDEXP15") + F.col("OPFEXP15") +
        F.col("ERDEXP15") + F.col("ERFEXP15"))
    .withColumn("PRESCRIBED_MEDICINES", F.col("RXEXP15"))
    .withColumn("DENTAL", F.col("DVTEXP15"))
    .withColumn("HOME_HEALTH_OTHER",
        F.col("HHAEXP15") + F.col("HHNEXP15") + F.col("OTHEXP15") + F.col("VISEXP15"))
    # Create flag variables for persons with an expense by type
    .withColumn("X_ANYSVCE", F.when(F.col("TOTAL") > 0, 1).otherwise(0))
    .withColumn("X_HOSPITAL_INPATIENT",
        F.when(F.col("HOSPITAL_INPATIENT") > 0, 1).otherwise(0))
    .withColumn("X_AMBULATORY",
        F.when(F.col("AMBULATORY") > 0, 1).otherwise(0))
    .withColumn("X_PRESCRIBED_MEDICINES",
        F.when(F.col("PRESCRIBED_MEDICINES") > 0, 1).otherwise(0))
    .withColumn("X_DENTAL",
        F.when(F.col("DENTAL") > 0, 1).otherwise(0))
    .withColumn("X_HOME_HEALTH_OTHER",
        F.when(F.col("HOME_HEALTH_OTHER") > 0, 1).otherwise(0))
    # Create age variable
    .withColumn("AGE",
        F.when(F.col("AGE15X") >= 0, F.col("AGE15X"))
        .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
        .when(F.col("AGE31X") >= 0, F.col("AGE31X")))
    .withColumn("AGECAT",
        F.when((F.col("AGE") >= 0) & (F.col("AGE") <= 64), 1)
        .when(F.col("AGE") > 64, 2))
    .withColumn("AGECAT_label",
        F.when(F.col("AGECAT") == 1, "0-64")
        .when(F.col("AGECAT") == 2, "65+"))
)

# (1) Percentage distribution of expenses by type of service
print("=== Percentage distribution of expenses by type of service ===")
exp_vars = ["HOSPITAL_INPATIENT", "AMBULATORY", "PRESCRIBED_MEDICINES",
            "DENTAL", "HOME_HEALTH_OTHER", "TOTAL"]
results_dist = survey_total(
    puf181,
    var_cols=exp_vars,
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
)
print(results_dist.to_string())

# (2) Percentage of persons with an expense, by type of service
print("\n=== Percentage of persons with an expense, by type of service ===")
flag_vars = ["X_ANYSVCE", "X_HOSPITAL_INPATIENT", "X_AMBULATORY",
             "X_PRESCRIBED_MEDICINES", "X_DENTAL", "X_HOME_HEALTH_OTHER"]
results_pct = survey_mean(
    puf181,
    var_cols=flag_vars,
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
)
print(results_pct.to_string())

# (3) Mean expense per person with an expense, by type of service and age
print("\n=== Mean total expense per person with an expense, by age ===")
results_mean = survey_mean(
    puf181.filter(F.col("X_ANYSVCE") == 1),
    var_cols=["TOTAL"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT15F",
    domain_col="AGECAT_label",
)
print(results_mean.to_string())

# Mean by type of service for persons with that type of expense
for service, flag in [
    ("HOSPITAL_INPATIENT", "X_HOSPITAL_INPATIENT"),
    ("AMBULATORY", "X_AMBULATORY"),
    ("PRESCRIBED_MEDICINES", "X_PRESCRIBED_MEDICINES"),
    ("DENTAL", "X_DENTAL"),
    ("HOME_HEALTH_OTHER", "X_HOME_HEALTH_OTHER"),
]:
    print(f"\n=== Mean {service} expense per person with expense, by age ===")
    results = survey_mean(
        puf181.filter(F.col(flag) == 1),
        var_cols=[service],
        stratum_col="VARSTR",
        cluster_col="VARPSU",
        weight_col="PERWT15F",
        domain_col="AGECAT_label",
    )
    print(results.to_string())

spark.stop()
