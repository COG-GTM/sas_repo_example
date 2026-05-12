"""
PySpark migration of pmed_prescribed_drug_2016.sas

DESCRIPTION: Prescribed drugs, 2016
  - Purchases and expenditures by generic drug name (RXDRGNAM)
  - Number of people with purchase
  - Total purchases
  - Total expenditures

Original: SAS/summary_tables_examples/pmed_prescribed_drug_2016.sas
Input: h188a.ssp (2016 RX event file)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT16F (weight)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum, count, lit


def run_etl(spark: SparkSession, rx_df: DataFrame) -> DataFrame:
    """
    Aggregate RX event file to person-by-drug level.

    Replicates the SAS PROC MEANS step that:
    - Groups by DUPERSID, VARSTR, VARPSU, PERWT16F, RXDRGNAM
    - Sums RXXP16X to get per-person expenditure by drug
    - Counts fills to get n_purchases per person per drug
    - Adds person=1 flag for counting unique persons
    """
    # Group by person + drug name, aggregate expenditures and fill counts
    rx_pers = (
        rx_df
        .groupBy("DUPERSID", "VARSTR", "VARPSU", "PERWT16F", "RXDRGNAM")
        .agg(
            spark_sum("RXXP16X").alias("pers_RXXP"),
            count("RXXP16X").alias("n_purchases")
        )
        .withColumn("person", lit(1))
    )

    return rx_pers
