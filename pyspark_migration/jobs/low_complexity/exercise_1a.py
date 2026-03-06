"""PySpark Migration: Exercise 1a - National Health Care Expenses, 2016.

Original: SAS/workshop_exercises/exercise_1a/Exercise1a.sas
          R/workshop_exercises/exercise_1a.R

Generates estimates on national health care expenses, 2016:
  (1) Overall expenses (national totals)
  (2) Percentage of persons with an expense
  (3) Mean expense per person with an expense, by age group (0-64, 65+)

Input: H192 (2016 Full-Year Consolidated file)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT16F (weight)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when


def run_etl(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """Execute the Exercise 1a ETL pipeline.

    Args:
        spark: Active SparkSession.
        input_path: Path to H192 data file (Parquet/CSV).
        output_path: Path to write output Parquet.

    Returns:
        Transformed DataFrame ready for survey estimation.
    """
    # Read 2016 Full-Year Consolidated file (HC-192)
    h192 = spark.read.parquet(input_path)

    # Select and keep only needed variables
    h192 = h192.select(
        "DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
        "VARSTR", "VARPSU", "PERWT16F"
    )

    # Create TOTAL variable
    h192 = h192.withColumn("TOTAL", col("TOTEXP16"))

    # Create flag for persons with any expense
    h192 = h192.withColumn(
        "X_ANYSVCE",
        when(col("TOTAL") > 0, 1).otherwise(0)
    )

    # Create summary AGE variable from end-of-year, round 4/2, round 3/1 variables
    h192 = h192.withColumn(
        "AGE",
        when(col("AGE16X") >= 0, col("AGE16X"))
        .when(col("AGE42X") >= 0, col("AGE42X"))
        .when(col("AGE31X") >= 0, col("AGE31X"))
    )

    # Create age category: 1 = 0-64, 2 = 65+
    h192 = h192.withColumn(
        "AGECAT",
        when((col("AGE") >= 0) & (col("AGE") <= 64), lit(1))
        .when(col("AGE") > 64, lit(2))
    )

    # Write output
    h192.write.mode("overwrite").parquet(output_path)

    return h192


def build_etl_dataframe(spark: SparkSession, input_path: str) -> DataFrame:
    """Build the ETL DataFrame without writing output (for testing).

    Args:
        spark: Active SparkSession.
        input_path: Path to H192 data file.

    Returns:
        Transformed DataFrame.
    """
    h192 = spark.read.parquet(input_path)

    h192 = h192.select(
        "DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
        "VARSTR", "VARPSU", "PERWT16F"
    )

    h192 = h192.withColumn("TOTAL", col("TOTEXP16"))
    h192 = h192.withColumn(
        "X_ANYSVCE",
        when(col("TOTAL") > 0, 1).otherwise(0)
    )
    h192 = h192.withColumn(
        "AGE",
        when(col("AGE16X") >= 0, col("AGE16X"))
        .when(col("AGE42X") >= 0, col("AGE42X"))
        .when(col("AGE31X") >= 0, col("AGE31X"))
    )
    h192 = h192.withColumn(
        "AGECAT",
        when((col("AGE") >= 0) & (col("AGE") <= 64), lit(1))
        .when(col("AGE") > 64, lit(2))
    )

    return h192
