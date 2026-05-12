"""
PySpark migration of Exercise1b.sas

DESCRIPTION: National health care expenses by type of service, 2015
  (1) Percentage distribution of expenses by type of service
  (2) Percentage of persons with an expense, by type of service
  (3) Mean expense per person with an expense, by type of service

Original: SAS/workshop_exercises/exercise_1b/Exercise1b.sas
Input: H181.SAS7BDAT (2015 Full-Year Consolidated)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT15F (weight)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when


def run_etl(spark: SparkSession, input_df: DataFrame) -> DataFrame:
    """
    Transform FYC 2015 data to compute expenditures by type of service.

    Replicates the SAS DATA step that:
    - Defines expenditure variables by type of service
    - Creates flag variables for persons with an expense by type
    - Derives AGE and AGECAT
    """
    result = input_df.select(
        "DUPERSID", "TOTEXP15", "IPDEXP15", "IPFEXP15", "OBVEXP15",
        "RXEXP15", "OPDEXP15", "OPFEXP15", "DVTEXP15", "ERDEXP15",
        "ERFEXP15", "HHAEXP15", "HHNEXP15", "OTHEXP15", "VISEXP15",
        "AGE15X", "AGE42X", "AGE31X", "VARSTR", "VARPSU", "PERWT15F"
    )

    # Define expenditure variables by type of service
    result = (
        result
        .withColumn("TOTAL", col("TOTEXP15"))
        .withColumn("HOSPITAL_INPATIENT", col("IPDEXP15") + col("IPFEXP15"))
        .withColumn("AMBULATORY",
                     col("OBVEXP15") + col("OPDEXP15") + col("OPFEXP15") +
                     col("ERDEXP15") + col("ERFEXP15"))
        .withColumn("PRESCRIBED_MEDICINES", col("RXEXP15"))
        .withColumn("DENTAL", col("DVTEXP15"))
        .withColumn("HOME_HEALTH_OTHER",
                     col("HHAEXP15") + col("HHNEXP15") + col("OTHEXP15") + col("VISEXP15"))
    )

    # Create flag (1/0) variables for persons with an expense
    expense_cols = [
        ("TOTAL", "X_ANYSVCE"),
        ("HOSPITAL_INPATIENT", "X_HOSPITAL_INPATIENT"),
        ("AMBULATORY", "X_AMBULATORY"),
        ("PRESCRIBED_MEDICINES", "X_PRESCRIBED_MEDICINES"),
        ("DENTAL", "X_DENTAL"),
        ("HOME_HEALTH_OTHER", "X_HOME_HEALTH_OTHER"),
    ]
    for exp_col, flag_col in expense_cols:
        result = result.withColumn(
            flag_col,
            when(col(exp_col) > 0, 1).otherwise(0)
        )

    # Derive AGE
    result = result.withColumn(
        "AGE",
        when(col("AGE15X") >= 0, col("AGE15X"))
        .when(col("AGE42X") >= 0, col("AGE42X"))
        .when(col("AGE31X") >= 0, col("AGE31X"))
    )

    # AGECAT: 1 = 0-64, 2 = 65+
    result = result.withColumn(
        "AGECAT",
        when((col("AGE") >= 0) & (col("AGE") <= 64), 1)
        .when(col("AGE") > 64, 2)
    )

    return result
