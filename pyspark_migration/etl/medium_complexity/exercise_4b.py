"""
PySpark migration of exercise_4b.R

DESCRIPTION: Regression for persons delaying medical care because of COVID, 2020
  - Percentage of people who delayed care
  - Logistic regression to identify demographic factors

Original: R/workshop_exercises/exercise_4b.R
Input: h224.dta (2020 Full-Year Consolidated)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT20F (weight)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when


def run_etl(spark: SparkSession, input_df: DataFrame) -> DataFrame:
    """
    Transform FYC 2020 data for COVID care delay analysis.

    Replicates the R script that:
    - Converts CVDLAY**53 from 1/2 to 0/1 for logistic regression
    - Creates subpop flags to exclude missings (CVDLAY** >= 0)
    """
    result = input_df.select(
        "DUPERSID", "VARPSU", "VARSTR", "PERWT20F",
        "CVDLAYCA53", "CVDLAYDN53", "CVDLAYPM53",
        "AGELAST", "SEX", "RACETHX", "INSCOV20", "REGION53"
    )

    # Convert outcomes from 1/2 to 1/0
    for var, new_var in [
        ("CVDLAYCA53", "covid_delay_CARE"),
        ("CVDLAYDN53", "covid_delay_DENTAL"),
        ("CVDLAYPM53", "covid_delay_PMED"),
    ]:
        result = result.withColumn(
            new_var,
            when(col(var) == 1, 1)
            .when(col(var) == 2, 0)
            .otherwise(col(var))
        )

    # Create subpopulation flags (exclude missings)
    result = (
        result
        .withColumn("subpop_CARE", (col("CVDLAYCA53") >= 0).cast("int"))
        .withColumn("subpop_DENTAL", (col("CVDLAYDN53") >= 0).cast("int"))
        .withColumn("subpop_PMED", (col("CVDLAYPM53") >= 0).cast("int"))
    )

    return result
