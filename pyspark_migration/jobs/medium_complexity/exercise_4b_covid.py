"""PySpark Migration: Exercise 4b - Regression for COVID Care Delay, 2020.

Original: R/workshop_exercises/exercise_4b.R

Includes a regression example for persons delaying medical care because of COVID:
  - Percentage of people who delayed care
  - Logistic regression: demographic factors associated with delayed care

Input: H224 (2020 Full-Year Consolidated file)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT20F (weight)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when


def run_etl(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """Execute the Exercise 4b COVID ETL pipeline.

    Args:
        spark: Active SparkSession.
        input_path: Path to H224 data file.
        output_path: Path to write output Parquet.

    Returns:
        Transformed DataFrame.
    """
    df = build_etl_dataframe(spark, input_path)
    df.write.mode("overwrite").parquet(output_path)
    return df


def build_etl_dataframe(spark: SparkSession, input_path: str) -> DataFrame:
    """Build the ETL DataFrame without writing output.

    Converts CVDLAY**53 from 1/2 to 0/1 for logistic regression and
    creates subpopulation indicators excluding missing values.

    Args:
        spark: Active SparkSession.
        input_path: Path to H224 data file.

    Returns:
        Transformed DataFrame with COVID delay variables.
    """
    fyc20 = spark.read.parquet(input_path)

    # Select needed variables
    fyc20 = fyc20.select(
        "DUPERSID", "VARPSU", "VARSTR", "PERWT20F",
        "CVDLAYCA53", "CVDLAYDN53", "CVDLAYPM53",
        "AGELAST", "SEX", "RACETHX", "INSCOV20", "REGION53"
    )

    # Convert outcomes from 1/2 to 0/1
    fyc20 = fyc20.withColumn(
        "covid_delay_CARE",
        when(col("CVDLAYCA53") == 1, 1)
        .when(col("CVDLAYCA53") == 2, 0)
        .otherwise(col("CVDLAYCA53"))
    )

    fyc20 = fyc20.withColumn(
        "covid_delay_DENTAL",
        when(col("CVDLAYDN53") == 1, 1)
        .when(col("CVDLAYDN53") == 2, 0)
        .otherwise(col("CVDLAYDN53"))
    )

    fyc20 = fyc20.withColumn(
        "covid_delay_PMED",
        when(col("CVDLAYPM53") == 1, 1)
        .when(col("CVDLAYPM53") == 2, 0)
        .otherwise(col("CVDLAYPM53"))
    )

    # Create subpopulation indicators (exclude missing values)
    fyc20 = fyc20.withColumn(
        "subpop_CARE", (col("CVDLAYCA53") >= 0).cast("int")
    )
    fyc20 = fyc20.withColumn(
        "subpop_DENTAL", (col("CVDLAYDN53") >= 0).cast("int")
    )
    fyc20 = fyc20.withColumn(
        "subpop_PMED", (col("CVDLAYPM53") >= 0).cast("int")
    )

    return fyc20
