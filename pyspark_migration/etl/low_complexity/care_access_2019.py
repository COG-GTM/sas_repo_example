"""
PySpark migration of care_access_2019.sas

DESCRIPTION: Access to Care, 2019
  - Did not receive treatment because couldn't afford it
  - Number/percent of people, by poverty status

Original: SAS/summary_tables_examples/care_access_2019.sas
Input: h216.sas7bdat (2019 Full-Year Consolidated)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT19F (weight)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when


def run_etl(spark: SparkSession, input_df: DataFrame) -> DataFrame:
    """
    Transform FYC 2019 data for care access analysis.

    Replicates the SAS DATA step that:
    - Creates afford_MD, afford_DN, afford_PM flags from AFRD* variables
    - Creates afford_ANY (any of the three)
    - Creates domain flag for access-to-care supplement eligibility (ACCELI42=1)
    - Adjusts weight: if domain=0 and PERWT19F=0 then PERWT19F=1
    """
    result = input_df.select(
        "DUPERSID", "AFRDCA42", "AFRDDN42", "AFRDPM42",
        "ACCELI42", "POVCAT19", "VARSTR", "VARPSU", "PERWT19F"
    )

    # Affordability flags
    result = (
        result
        .withColumn("afford_MD", (col("AFRDCA42") == 1).cast("int"))
        .withColumn("afford_DN", (col("AFRDDN42") == 1).cast("int"))
        .withColumn("afford_PM", (col("AFRDPM42") == 1).cast("int"))
    )
    result = result.withColumn(
        "afford_ANY",
        ((col("afford_MD") == 1) | (col("afford_DN") == 1) | (col("afford_PM") == 1)).cast("int")
    )

    # Domain: persons eligible for access-to-care supplement
    result = result.withColumn("domain", (col("ACCELI42") == 1).cast("int"))

    # Adjust weight so SAS doesn't drop observations
    result = result.withColumn(
        "PERWT19F",
        when((col("domain") == 0) & (col("PERWT19F") == 0), 1)
        .otherwise(col("PERWT19F"))
    )

    return result
