"""PySpark Migration: Prescribed Drug Purchases/Expenditures, 2016.

Original: SAS/summary_tables_examples/pmed_prescribed_drug_2016.sas

Replicates estimates from MEPS-HC Data Tools summary tables:
  - Purchases and expenditures by generic drug name (RXDRGNAM)
  - Number of people with purchase
  - Total purchases
  - Total expenditures

Input: H188A (2016 RX event file)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT16F (weight)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, lit, sum as spark_sum


def run_etl(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """Execute the Prescribed Drug 2016 ETL pipeline.

    Args:
        spark: Active SparkSession.
        input_path: Path to H188A RX event file.
        output_path: Path to write output Parquet.

    Returns:
        Person-drug level aggregated DataFrame.
    """
    df = build_etl_dataframe(spark, input_path)
    df.write.mode("overwrite").parquet(output_path)
    return df


def build_etl_dataframe(spark: SparkSession, input_path: str) -> DataFrame:
    """Build the ETL DataFrame without writing output.

    Replicates the SAS PROC SORT + PROC MEANS aggregation:
      - Group by DUPERSID, VARSTR, VARPSU, PERWT16F, RXDRGNAM
      - Sum RXXP16X -> pers_RXXP (person-drug total expenditure)
      - Count RXXP16X -> n_purchases (number of fills)
      - Add person indicator = 1

    Args:
        spark: Active SparkSession.
        input_path: Path to H188A data file.

    Returns:
        Person-drug level aggregated DataFrame.
    """
    rx = spark.read.parquet(input_path)

    # Aggregate to person-drug level (matching SAS PROC MEANS BY statement)
    rx_pers = rx.groupBy(
        "DUPERSID", "VARSTR", "VARPSU", "PERWT16F", "RXDRGNAM"
    ).agg(
        spark_sum("RXXP16X").alias("pers_RXXP"),
        count("RXXP16X").alias("n_purchases")
    ).withColumn("person", lit(1))

    return rx_pers
