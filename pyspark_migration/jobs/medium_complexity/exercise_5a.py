"""PySpark Migration: Exercise 5a - Family-Level Variables from Person-Level Data, 2015.

Original: SAS/workshop_exercises/exercise_5a/Exercise5a.sas

Illustrates how to construct family-level variables from person-level data:
  - Uses CPS Family definition: ID = DUID + CPSFAMID, Weight = FAMWT15C
  - Computes family-level aggregates:
    - FAMSIZE: number of persons per CPS family
    - FAMOOP: total out-of-pocket expenditure (TOTSLF15) per CPS family
    - FAMINC: total income (TTLP15X) per CPS family

Input: H181 (2015 Full-Year Consolidated file)
Survey design: VARSTR (strata), VARPSU (cluster), FAMWT15C (weight)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, first, sum as spark_sum
from pyspark.sql.window import Window


def run_etl(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """Execute the Exercise 5a ETL pipeline.

    Args:
        spark: Active SparkSession.
        input_path: Path to H181 data file.
        output_path: Path to write output Parquet.

    Returns:
        Family-level DataFrame.
    """
    df = build_etl_dataframe(spark, input_path)
    df.write.mode("overwrite").parquet(output_path)
    return df


def build_etl_dataframe(spark: SparkSession, input_path: str) -> DataFrame:
    """Build the family-level ETL DataFrame without writing output.

    Args:
        spark: Active SparkSession.
        input_path: Path to H181 data file.

    Returns:
        Family-level DataFrame with FAMSIZE, FAMOOP, FAMINC.
    """
    h181 = spark.read.parquet(input_path)

    # Select needed variables
    pers = h181.select(
        "DUPERSID", "DUID", "CPSFAMID", "FAMWT15C",
        "VARSTR", "VARPSU", "TOTSLF15", "TTLP15X"
    )

    # Aggregate to family level: group by DUID + CPSFAMID
    fam = pers.groupBy("DUID", "CPSFAMID").agg(
        count("*").alias("FAMSIZE"),
        spark_sum("TOTSLF15").alias("FAMOOP"),
        spark_sum("TTLP15X").alias("FAMINC"),
    )

    # Get family weight, VARSTR, VARPSU from first member with positive weight
    famwt = (
        pers
        .filter(col("FAMWT15C") > 0)
        .dropDuplicates(["DUID", "CPSFAMID"])
        .select("DUID", "CPSFAMID", "FAMWT15C", "VARSTR", "VARPSU")
    )

    # Join family aggregates with weights
    fam2 = fam.join(famwt, on=["DUID", "CPSFAMID"], how="inner")

    return fam2
