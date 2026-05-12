"""
PySpark migration of ins_age_2016.sas

DESCRIPTION: Health insurance, 2016
  - Number/percent of people by insurance coverage and age groups

Original: SAS/summary_tables_examples/ins_age_2016.sas
Input: h192.ssp (2016 Full-Year Consolidated)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT16F (weight)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when


def run_etl(spark: SparkSession, input_df: DataFrame) -> DataFrame:
    """
    Transform FYC 2016 data for insurance-by-age cross-tabulation.

    Replicates the SAS PROC SURVEYFREQ step that:
    - Cross-tabulates AGELAST by INSURC16 with survey weights
    - Uses age groups: Under 5, 5-17, 18-44, 45-64, 65+
    - Uses insurance categories from INSURC16
    """
    result = input_df.select(
        "DUPERSID", "AGELAST", "INSURC16", "VARSTR", "VARPSU", "PERWT16F"
    )

    # Age groups matching SAS format
    result = result.withColumn(
        "AGE_GRP",
        when(col("AGELAST") < 5, "Under 5")
        .when((col("AGELAST") >= 5) & (col("AGELAST") <= 17), "5-17")
        .when((col("AGELAST") >= 18) & (col("AGELAST") <= 44), "18-44")
        .when((col("AGELAST") >= 45) & (col("AGELAST") <= 64), "45-64")
        .when(col("AGELAST") >= 65, "65+")
    )

    # Insurance category labels
    result = result.withColumn(
        "INS_CATEGORY",
        when(col("INSURC16") == 1, "<65, Any private")
        .when(col("INSURC16") == 2, "<65, Public only")
        .when(col("INSURC16") == 3, "<65, Uninsured")
        .when(col("INSURC16") == 4, "65+, Medicare only")
        .when(col("INSURC16") == 5, "65+, Medicare and private")
        .when(col("INSURC16") == 6, "65+, Medicare and other public")
        .when(col("INSURC16").isin(7, 8), "65+, No medicare")
    )

    return result
