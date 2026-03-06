"""PySpark Migration: Insurance by Age Group, 2016.

Original: SAS/summary_tables_examples/ins_age_2016.sas

Replicates estimates from MEPS-HC Data Tools summary tables:
  - Health insurance coverage, 2016
  - Number/percent of people by insurance coverage and age groups

Input: H192 (2016 Full-Year Consolidated file)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT16F (weight)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when


def run_etl(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """Execute the Insurance by Age 2016 ETL pipeline.

    Args:
        spark: Active SparkSession.
        input_path: Path to H192 data file.
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
        input_path: Path to H192 data file.

    Returns:
        Transformed DataFrame.
    """
    h192 = spark.read.parquet(input_path)

    h192 = h192.select(
        "DUPERSID", "AGELAST", "INSURC16", "VARSTR", "VARPSU", "PERWT16F"
    )

    # Create age group categories matching SAS format
    h192 = h192.withColumn(
        "AGE_GRP",
        when(col("AGELAST") < 5, "Under 5")
        .when((col("AGELAST") >= 5) & (col("AGELAST") <= 17), "5-17")
        .when((col("AGELAST") >= 18) & (col("AGELAST") <= 44), "18-44")
        .when((col("AGELAST") >= 45) & (col("AGELAST") <= 64), "45-64")
        .when(col("AGELAST") >= 65, "65+")
    )

    # Create insurance category labels
    h192 = h192.withColumn(
        "INS_CAT",
        when(col("INSURC16") == 1, "<65, Any private")
        .when(col("INSURC16") == 2, "<65, Public only")
        .when(col("INSURC16") == 3, "<65, Uninsured")
        .when(col("INSURC16") == 4, "65+, Medicare only")
        .when(col("INSURC16") == 5, "65+, Medicare and private")
        .when(col("INSURC16") == 6, "65+, Medicare and other public")
        .when(
            (col("INSURC16") == 7) | (col("INSURC16") == 8),
            "65+, No medicare"
        )
    )

    return h192
