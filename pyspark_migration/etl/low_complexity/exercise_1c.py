"""
PySpark migration of Exercise1c.sas

DESCRIPTION: National health care expenses, 2018
  - Overall expenses (National totals)
  - Percentage of persons with an expense
  - Mean expense per person
  - Mean/median expense per person with an expense by age group

Original: SAS/workshop_exercises/exercise_1c/Exercise1c.sas
Input: H209V9 (2018 Full-Year Consolidated)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT18F (weight)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit


def run_etl(spark: SparkSession, input_df: DataFrame) -> DataFrame:
    """
    Transform FYC 2018 data.

    Replicates the SAS DATA step that:
    - Creates WITH_AN_EXPENSE from TOTEXP18
    - Creates CHAR_WITH_AN_EXPENSE category ('No Expense' / 'Any Expense')
    - Uses AGELAST directly (no need to derive from multiple age vars post-2007)
    """
    result = input_df.select(
        "DUPERSID", "TOTEXP18", "AGELAST", "VARSTR", "VARPSU", "PERWT18F"
    )

    # WITH_AN_EXPENSE = TOTEXP18 (another version of the variable)
    result = result.withColumn("WITH_AN_EXPENSE", col("TOTEXP18"))

    # CHAR_WITH_AN_EXPENSE: category variable
    result = result.withColumn(
        "CHAR_WITH_AN_EXPENSE",
        when(col("TOTEXP18") == 0, "No Expense").otherwise("Any Expense")
    )

    # AGECAT for domain analysis: 0-64, 65+
    result = result.withColumn(
        "AGECAT",
        when(col("AGELAST") <= 64, "0-64").otherwise("65+")
    )

    return result
