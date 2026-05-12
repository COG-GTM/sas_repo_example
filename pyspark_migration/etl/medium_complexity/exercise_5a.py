"""
PySpark migration of Exercise5a.sas

DESCRIPTION: Construct family-level variables from person-level data
  - CPS Family: ID is DUID + CPSFAMID, weight is FAMWT15C
  - Family size, total out-of-pocket expenditure, total income

Original: SAS/workshop_exercises/exercise_5a/Exercise5a.sas
Input: H181.SAS7BDAT (2015 FY PUF)
Survey design: VARSTR (strata), VARPSU (cluster), FAMWT15C (weight)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, first


def run_etl(spark: SparkSession, input_df: DataFrame) -> DataFrame:
    """
    Construct family-level aggregates from person-level data.

    Replicates the SAS DATA step that:
    - Sorts by DUID, CPSFAMID
    - Accumulates FAMSIZE, FAMOOP (TOTSLF15), FAMINC (TTLP15X)
    - Outputs one row per family (last person in CPSFAMID group)
    - Merges back with family weight (FAMWT15C > 0)
    """
    pers = input_df.select(
        "DUPERSID", "DUID", "CPSFAMID", "FAMWT15C",
        "VARSTR", "VARPSU", "TOTSLF15", "TTLP15X"
    )

    # Aggregate to family level
    fam = (
        pers
        .groupBy("DUID", "CPSFAMID")
        .agg(
            count("*").alias("FAMSIZE"),
            spark_sum("TOTSLF15").alias("FAMOOP"),
            spark_sum("TTLP15X").alias("FAMINC")
        )
    )

    # Get family weight and survey design variables (one per family, weight > 0)
    famwt = (
        pers
        .filter(col("FAMWT15C") > 0)
        .groupBy("DUID", "CPSFAMID")
        .agg(
            first("FAMWT15C").alias("FAMWT15C"),
            first("VARSTR").alias("VARSTR"),
            first("VARPSU").alias("VARPSU")
        )
    )

    # Merge family aggregates with weights (inner join = both must exist)
    result = fam.join(famwt, on=["DUID", "CPSFAMID"], how="inner")

    return result
