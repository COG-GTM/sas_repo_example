"""
PySpark migration of cond_pmed_2020.sas

DESCRIPTION: Prescribed medicine utilization and expenditures for the treatment
  of hyperlipidemia, 2020

  National totals:
  - Total number of people with at least one PMED fill for hyperlipidemia (HL)
  - Total PMED fills for HL
  - Total PMED expenditures for HL

  Per-person averages (among those with 1+ fill):
  - Avg PMED fills for HL, by sex and poverty (POVCAT20)
  - Avg PMED expenditures for HL, by sex and poverty (POVCAT20)

Original: SAS/workshop_exercises/cond_pmed_2020.sas
Inputs:
  - h220a.sas7bdat  (2020 Prescribed Medicines)
  - h222.sas7bdat   (2020 Conditions)
  - h220if1.sas7bdat (2020 CLNK: Condition-Event Link)
  - h224.sas7bdat   (2020 Full-Year Consolidated)

Survey design: VARSTR (strata), VARPSU (cluster), PERWT20F (weight)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, count, sum as spark_sum, lit


def load_and_prepare(
    spark: SparkSession,
    pmed_df: DataFrame,
    cond_df: DataFrame,
    clnk_df: DataFrame,
    fyc_df: DataFrame
) -> dict:
    """
    Execute the full 4-file join chain with de-duplication.

    Returns a dict of intermediate DataFrames for testing checkpoints:
    - 'hl_cond': Conditions filtered to hyperlipidemia
    - 'cond_clnk': After CLNK join
    - 'cond_clnk_dedup': After EVNTIDX de-duplication
    - 'hl_merged': After PMED join
    - 'person_level': After person-level collapse
    - 'result': Final FYC left join with zero-fill
    """
    intermediates = {}

    # Prepare PMED: rename LINKIDX to EVNTIDX
    pmed = pmed_df.select(
        "DUPERSID", "DRUGIDX", "RXRECIDX",
        col("LINKIDX").alias("EVNTIDX"),
        "RXDRGNAM", "RXXP20X"
    )

    # Prepare Conditions: keep needed columns
    cond = cond_df.select("DUPERSID", "CONDIDX", "ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X")

    # Prepare CLNK
    clnk = clnk_df.select("DUPERSID", "CONDIDX", "EVNTIDX", "EVENTYPE")

    # Prepare FYC: keep needed columns
    fyc = fyc_df.select(
        "DUPERSID", "AGELAST", "SEX", "POVCAT20", "CHOLDX",
        "PERWT20F", "VARPSU", "VARSTR"
    )

    # Step 1: Filter conditions to hyperlipidemia (any CCSR = END010)
    hl_cond = cond.filter(
        (col("CCSR1X") == "END010") |
        (col("CCSR2X") == "END010") |
        (col("CCSR3X") == "END010")
    )
    intermediates["hl_cond"] = hl_cond

    # Step 2: Join conditions to CLNK by DUPERSID + CONDIDX (inner join)
    cond_clnk = hl_cond.join(clnk, on=["DUPERSID", "CONDIDX"], how="inner")
    intermediates["cond_clnk"] = cond_clnk

    # Step 3: CRITICAL - De-duplicate on DUPERSID + EVNTIDX
    # Matches SAS: proc sort nodupkey by dupersid evntidx
    cond_clnk_dedup = cond_clnk.dropDuplicates(["DUPERSID", "EVNTIDX"])
    intermediates["cond_clnk_dedup"] = cond_clnk_dedup

    # Step 4: Join to PMED by DUPERSID + EVNTIDX (inner join)
    hl_merged = cond_clnk_dedup.join(pmed, on=["DUPERSID", "EVNTIDX"], how="inner")
    intermediates["hl_merged"] = hl_merged

    # Step 5: Collapse to person level - sum fills and expenditures
    person_level = (
        hl_merged
        .groupBy("DUPERSID")
        .agg(
            count("*").alias("n_hl_fills"),
            spark_sum("RXXP20X").alias("hl_drug_exp")
        )
    )
    intermediates["person_level"] = person_level

    # Step 6: Left join to FYC, fill zeros for non-matched
    result = (
        fyc.join(person_level, on="DUPERSID", how="left")
        .fillna({"n_hl_fills": 0, "hl_drug_exp": 0.0})
        .withColumn("hl_pmed_flag", when(col("n_hl_fills") > 0, 1).otherwise(0))
    )
    intermediates["result"] = result

    return intermediates


def run_etl(
    spark: SparkSession,
    pmed_df: DataFrame,
    cond_df: DataFrame,
    clnk_df: DataFrame,
    fyc_df: DataFrame
) -> DataFrame:
    """Run the full ETL and return the final result DataFrame."""
    intermediates = load_and_prepare(spark, pmed_df, cond_df, clnk_df, fyc_df)
    return intermediates["result"]
