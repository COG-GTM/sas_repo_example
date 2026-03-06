"""PySpark Migration: Exercise 4a - Pooling FYC Files 2015+2016.

Original: SAS/workshop_exercises/exercise_4a/Exercise4a.sas

Illustrates how to pool MEPS data files from different years.
Example: Population age 26-30 who are uninsured but have high income.

Variables with year-specific names are renamed before combining:
  INSCOV15/INSCOV16 -> INSCOV
  PERWT15F/PERWT16F -> PERWT
  POVCAT15/POVCAT16 -> POVCAT
  TOTSLF15/TOTSLF16 -> TOTSLF

Pooled weight = PERWT / 2

Input: H192 (2016 FYC), H181 (2015 FYC)
Survey design: VARSTR (strata), VARPSU (cluster), POOLWT (weight)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when


def run_etl(
    spark: SparkSession,
    input_path_2015: str,
    input_path_2016: str,
    output_path: str,
) -> DataFrame:
    """Execute the Exercise 4a ETL pipeline.

    Args:
        spark: Active SparkSession.
        input_path_2015: Path to H181 (2015 FYC) data file.
        input_path_2016: Path to H192 (2016 FYC) data file.
        output_path: Path to write output Parquet.

    Returns:
        Pooled DataFrame.
    """
    df = build_etl_dataframe(spark, input_path_2015, input_path_2016)
    df.write.mode("overwrite").parquet(output_path)
    return df


def build_etl_dataframe(
    spark: SparkSession,
    input_path_2015: str,
    input_path_2016: str,
) -> DataFrame:
    """Build the pooled ETL DataFrame without writing output.

    Args:
        spark: Active SparkSession.
        input_path_2015: Path to H181 (2015 FYC) data file.
        input_path_2016: Path to H192 (2016 FYC) data file.

    Returns:
        Pooled DataFrame with renamed variables and pooled weight.
    """
    # Read 2015 FYC, filter to positive weights, rename year-specific vars
    yr1 = spark.read.parquet(input_path_2015).select(
        "DUPERSID", "INSCOV15", "PERWT15F", "VARSTR", "VARPSU",
        "POVCAT15", "AGELAST", "TOTSLF15"
    ).filter(col("PERWT15F") > 0)

    yr1 = (
        yr1
        .withColumnRenamed("INSCOV15", "INSCOV")
        .withColumnRenamed("PERWT15F", "PERWT")
        .withColumnRenamed("POVCAT15", "POVCAT")
        .withColumnRenamed("TOTSLF15", "TOTSLF")
    )

    # Read 2016 FYC, filter to positive weights, rename year-specific vars
    yr2 = spark.read.parquet(input_path_2016).select(
        "DUPERSID", "INSCOV16", "PERWT16F", "VARSTR", "VARPSU",
        "POVCAT16", "AGELAST", "TOTSLF16"
    ).filter(col("PERWT16F") > 0)

    yr2 = (
        yr2
        .withColumnRenamed("INSCOV16", "INSCOV")
        .withColumnRenamed("PERWT16F", "PERWT")
        .withColumnRenamed("POVCAT16", "POVCAT")
        .withColumnRenamed("TOTSLF16", "TOTSLF")
    )

    # Stack years and create pooled weight
    pool = yr1.unionByName(yr2).withColumn("POOLWT", col("PERWT") / 2)

    # Create subpopulation flag: age 26-30, high income (POVCAT=5), uninsured (INSCOV=3)
    pool = pool.withColumn(
        "SUBPOP",
        when(
            (col("AGELAST") >= 26)
            & (col("AGELAST") <= 30)
            & (col("POVCAT") == 5)
            & (col("INSCOV") == 3),
            1
        ).otherwise(2)
    )

    return pool
