"""
PySpark migration of Exercise4a.sas

DESCRIPTION: Pool MEPS data files from different years (2015 and 2016)
  - Population age 26-30, uninsured, high income
  - Weighted estimate on TOTSLF (total amount paid by self/family)

Original: SAS/workshop_exercises/exercise_4a/Exercise4a.sas
Inputs: H192.SAS7BDAT (2016 FYC), H181.SAS7BDAT (2015 FYC)
Survey design: VARSTR (strata), VARPSU (cluster), POOLWT (pooled weight)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit


def run_etl(spark: SparkSession, yr1_df: DataFrame, yr2_df: DataFrame) -> DataFrame:
    """
    Pool 2015 and 2016 FYC files.

    Replicates the SAS DATA steps that:
    - Select and rename year-specific variables
    - Filter to persons with positive weight
    - Stack (union) the two years
    - Create pooled weight = PERWT / 2
    - Create SUBPOP flag for age 26-30, high income (POVCAT=5), uninsured (INSCOV=3)
    """
    # 2015: select and rename year-specific variables
    yr1 = (
        yr1_df
        .select("DUPERSID", "INSCOV15", "PERWT15F", "VARSTR", "VARPSU",
                "POVCAT15", "AGELAST", "TOTSLF15")
        .filter(col("PERWT15F") > 0)
        .withColumnRenamed("INSCOV15", "INSCOV")
        .withColumnRenamed("PERWT15F", "PERWT")
        .withColumnRenamed("POVCAT15", "POVCAT")
        .withColumnRenamed("TOTSLF15", "TOTSLF")
    )

    # 2016: select and rename year-specific variables
    yr2 = (
        yr2_df
        .select("DUPERSID", "INSCOV16", "PERWT16F", "VARSTR", "VARPSU",
                "POVCAT16", "AGELAST", "TOTSLF16")
        .filter(col("PERWT16F") > 0)
        .withColumnRenamed("INSCOV16", "INSCOV")
        .withColumnRenamed("PERWT16F", "PERWT")
        .withColumnRenamed("POVCAT16", "POVCAT")
        .withColumnRenamed("TOTSLF16", "TOTSLF")
    )

    # Stack and create pooled weight
    pool = yr1.union(yr2).withColumn("POOLWT", col("PERWT") / 2)

    # SUBPOP: age 26-30, high income (POVCAT=5), uninsured (INSCOV=3)
    pool = pool.withColumn(
        "SUBPOP",
        when(
            (col("AGELAST") >= 26) & (col("AGELAST") <= 30) &
            (col("POVCAT") == 5) & (col("INSCOV") == 3),
            1
        ).otherwise(2)
    )

    return pool
