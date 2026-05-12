"""
PySpark migration of Exercise1a.sas

DESCRIPTION: National health care expenses, 2016
  (1) Overall expenses
  (2) Percentage of persons with an expense
  (3) Mean expense per person with an expense

Original: SAS/workshop_exercises/exercise_1a/Exercise1a.sas
Input: H192.SAS7BDAT (2016 Full-Year Consolidated)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT16F (weight)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit


def run_etl(spark: SparkSession, input_df: DataFrame) -> DataFrame:
    """
    Transform FYC 2016 data to compute expense and age category variables.

    Replicates the SAS DATA step that:
    - Creates TOTAL from TOTEXP16
    - Creates X_ANYSVCE flag (1 if TOTAL > 0)
    - Derives AGE from AGE16X/AGE42X/AGE31X
    - Creates AGECAT (1=0-64, 2=65+)
    """
    result = input_df.select(
        "DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
        "VARSTR", "VARPSU", "PERWT16F"
    )

    # TOTAL = TOTEXP16
    result = result.withColumn("TOTAL", col("TOTEXP16"))

    # X_ANYSVCE flag: 1 if TOTAL > 0, else 0
    result = result.withColumn(
        "X_ANYSVCE",
        when(col("TOTAL") > 0, 1).otherwise(0)
    )

    # Derive AGE from end-of-year, round 4/2, round 3/1 variables
    result = result.withColumn(
        "AGE",
        when(col("AGE16X") >= 0, col("AGE16X"))
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
