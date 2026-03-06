"""PySpark Migration: Care Access 2019 - Access to Care by Poverty Status.

Original: SAS/summary_tables_examples/care_access_2019.sas

Replicates estimates from MEPS-HC Data Tools summary tables:
  - Did not receive treatment because couldn't afford it
  - Number/percent of people, by poverty status

Input: H216 (2019 Full-Year Consolidated file)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT19F (weight)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when


def run_etl(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """Execute the Care Access 2019 ETL pipeline.

    Args:
        spark: Active SparkSession.
        input_path: Path to H216 data file.
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
        input_path: Path to H216 data file.

    Returns:
        Transformed DataFrame.
    """
    h216 = spark.read.parquet(input_path)

    # Didn't receive care because couldn't afford it
    h216 = h216.withColumn(
        "afford_MD", (col("AFRDCA42") == 1).cast("int")
    )
    h216 = h216.withColumn(
        "afford_DN", (col("AFRDDN42") == 1).cast("int")
    )
    h216 = h216.withColumn(
        "afford_PM", (col("AFRDPM42") == 1).cast("int")
    )
    h216 = h216.withColumn(
        "afford_ANY",
        (
            (col("afford_MD") == 1)
            | (col("afford_DN") == 1)
            | (col("afford_PM") == 1)
        ).cast("int")
    )

    # Define domain: persons eligible for access to care supplement
    h216 = h216.withColumn(
        "domain", (col("ACCELI42") == 1).cast("int")
    )

    # Adjust weights: if domain=0 and weight=0, set weight to 1
    # so SAS/survey procedures don't drop observations
    h216 = h216.withColumn(
        "PERWT19F",
        when(
            (col("domain") == 0) & (col("PERWT19F") == 0), 1
        ).otherwise(col("PERWT19F"))
    )

    return h216
