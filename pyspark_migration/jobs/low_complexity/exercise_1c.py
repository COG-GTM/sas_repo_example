"""PySpark Migration: Exercise 1c - National Health Care Expenses, 2018.

Original: SAS/workshop_exercises/exercise_1c/Exercise1c.sas

Generates estimates on national health care expenses, 2018:
  - Overall expenses (national totals)
  - Percentage of persons with an expense
  - Mean expense per person
  - Mean/median expense per person with an expense, by age group (0-64, 65+)

Input: H209 (2018 Full-Year Consolidated file)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT18F (weight)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when


def run_etl(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """Execute the Exercise 1c ETL pipeline.

    Args:
        spark: Active SparkSession.
        input_path: Path to H209 data file.
        output_path: Path to write output Parquet.

    Returns:
        Transformed DataFrame.
    """
    df = build_etl_dataframe(spark, input_path)
    df.write.mode("overwrite").parquet(output_path)
    return df


def build_etl_dataframe(spark: SparkSession, input_path: str) -> DataFrame:
    """Build the ETL DataFrame without writing output.

    Args:
        spark: Active SparkSession.
        input_path: Path to H209 data file.

    Returns:
        Transformed DataFrame.
    """
    h209 = spark.read.parquet(input_path)

    h209 = h209.select(
        "DUPERSID", "TOTEXP18", "AGELAST", "VARSTR", "VARPSU", "PERWT18F", "PANEL"
    )

    # Create WITH_AN_EXPENSE (copy of TOTEXP18)
    h209 = h209.withColumn("WITH_AN_EXPENSE", col("TOTEXP18"))

    # Create character version: 'No Expense' or 'Any Expense'
    h209 = h209.withColumn(
        "CHAR_WITH_AN_EXPENSE",
        when(col("TOTEXP18") == 0, "No Expense").otherwise("Any Expense")
    )

    # Create age category for domain analysis
    h209 = h209.withColumn(
        "AGECAT",
        when(col("AGELAST") <= 64, "0-64").otherwise("65+")
    )

    return h209
