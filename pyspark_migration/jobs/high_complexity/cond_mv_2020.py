"""PySpark Migration: Office-Based Visits for Mental Health, 2020.

Original: Stata/workshop_exercises/cond_mv_2020.do

NOTE: The Stata file cond_mv_2020.do actually implements the same hyperlipidemia
PMED analysis as cond_pmed_2020.sas, not mental health office visits.
This module mirrors the Stata script's logic exactly.

Links MEPS-HC Medical Conditions, CLNK, Prescribed Medicines, and FYC files
for data year 2020 to estimate:
  - Total number of people with 1+ Rx fills for hyperlipidemia
  - Total Rx fills for treatment of hyperlipidemia
  - Total Rx expenditures for treatment of hyperlipidemia
  - Mean number of Rx fills per person (among those with any), by sex and income
  - Mean expenditures per person (among those with any), by sex and income

Join chain (4-file):
  1. Conditions (h222) -> filter to CCSR=END010 (hyperlipidemia)
  2. CLNK (h220if1) -> merge m:m on condidx
  3. CRITICAL: duplicates drop evntidx, force
  4. PMED (h220a) -> merge 1:m on evntidx
  5. Collapse to person-level (sum fills + expenditures)
  6. Merge 1:1 to FYC, fill zeros

Input files:
  - h220a (2020 Prescribed Medicines file)
  - h222 (2020 Conditions file)
  - h220if1 (2020 CLNK file)
  - h224 (2020 Full-Year Consolidated file)

Survey design: VARSTR (strata), VARPSU (cluster), PERWT20F (weight)
"""

from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when


def run_etl(
    spark: SparkSession,
    pmed_path: str,
    cond_path: str,
    clnk_path: str,
    fyc_path: str,
    output_path: str,
) -> DataFrame:
    """Execute the cond_mv_2020 (Stata) ETL pipeline.

    Args:
        spark: Active SparkSession.
        pmed_path: Path to h220a.
        cond_path: Path to h222.
        clnk_path: Path to h220if1.
        fyc_path: Path to h224.
        output_path: Path to write output Parquet.

    Returns:
        Final person-level DataFrame.
    """
    result = build_etl_dataframe(spark, pmed_path, cond_path, clnk_path, fyc_path)
    result.write.mode("overwrite").parquet(output_path)
    return result


def build_etl_dataframe(
    spark: SparkSession,
    pmed_path: str,
    cond_path: str,
    clnk_path: str,
    fyc_path: str,
) -> DataFrame:
    """Build the full ETL DataFrame without writing output.

    Args:
        spark: Active SparkSession.
        pmed_path: Path to h220a.
        cond_path: Path to h222.
        clnk_path: Path to h220if1.
        fyc_path: Path to h224.

    Returns:
        Person-level DataFrame.
    """
    intermediates = build_etl_with_intermediates(
        spark, pmed_path, cond_path, clnk_path, fyc_path
    )
    return intermediates["result"]


def build_etl_with_intermediates(
    spark: SparkSession,
    pmed_path: str,
    cond_path: str,
    clnk_path: str,
    fyc_path: str,
) -> Dict[str, DataFrame]:
    """Build ETL with all intermediate DataFrames for testing.

    Mirrors the Stata script step by step:
      1. Load CLNK, rename to lower
      2. Load FYC (selected vars), create HL_ever flag
      3. Load PMED, rename linkidx -> evntidx
      4. Load Conditions, filter to END010
      5. Merge conditions to CLNK on condidx (m:m)
      6. Drop unmatched, de-duplicate on evntidx
      7. Merge to PMED on evntidx (1:m), drop unmatched
      8. Collapse to person-level
      9. Merge to FYC, fill zeros, create any_rx flag

    Returns:
        Dictionary of intermediate DataFrames.
    """
    intermediates = {}

    # Load CLNK
    clnk = spark.read.parquet(clnk_path)
    intermediates["clnk"] = clnk

    # Load FYC with selected variables
    fyc = spark.read.parquet(fyc_path).select(
        "DUPERSID", "SEX", "AGELAST", "CHOLDX", "POVCAT20",
        "VARSTR", "VARPSU", "PERWT20F"
    )
    # Create HL_ever flag: CHOLDX=1 -> 1, CHOLDX=2 -> 0, else null
    fyc = fyc.withColumn(
        "HL_ever",
        when(col("CHOLDX") == 1, 1)
        .when(col("CHOLDX") == 2, 0)
    )
    intermediates["fyc"] = fyc

    # Load PMED, rename LINKIDX -> EVNTIDX
    pmed = spark.read.parquet(pmed_path).select(
        "DUPERSID", "DRUGIDX", "RXRECIDX", "LINKIDX", "RXDRGNAM", "RXXP20X"
    ).withColumnRenamed("LINKIDX", "EVNTIDX")
    intermediates["pmed"] = pmed

    # Load Conditions, filter to hyperlipidemia (END010)
    cond = spark.read.parquet(cond_path).select(
        "DUPERSID", "CONDIDX", "ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"
    )
    hl_cond = cond.filter(
        (col("CCSR1X") == "END010")
        | (col("CCSR2X") == "END010")
        | (col("CCSR3X") == "END010")
    )
    intermediates["hl_cond"] = hl_cond

    # Merge conditions to CLNK (Stata: merge m:m condidx)
    # In Stata this is merge m:m condidx, which is an inner join
    cond_clnk = hl_cond.join(clnk, on=["DUPERSID", "CONDIDX"], how="inner")
    intermediates["cond_clnk"] = cond_clnk

    # CRITICAL: De-duplicate on EVNTIDX (Stata: duplicates drop evntidx, force)
    cond_clnk_dedup = cond_clnk.dropDuplicates(["EVNTIDX"])
    intermediates["cond_clnk_dedup"] = cond_clnk_dedup

    # Merge to PMED (Stata: merge 1:m evntidx)
    linked = cond_clnk_dedup.join(pmed, on=["DUPERSID", "EVNTIDX"], how="inner")
    intermediates["linked"] = linked

    # Collapse to person-level (Stata: collapse (sum) num_rx=one (sum) exp_rx=rxxp20x)
    person_level = linked.groupBy("DUPERSID").agg(
        count("*").alias("num_rx"),
        spark_sum("RXXP20X").alias("exp_rx"),
    )
    intermediates["person_level"] = person_level

    # Merge to FYC (Stata: merge 1:1 dupersid using FY_2020)
    result = fyc.join(person_level, on="DUPERSID", how="left")
    result = result.fillna({"num_rx": 0, "exp_rx": 0.0})
    result = result.withColumn(
        "any_rx", (col("num_rx") > 0).cast("int")
    )
    intermediates["result"] = result

    return intermediates
