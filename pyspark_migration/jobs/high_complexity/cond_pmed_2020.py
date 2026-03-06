"""PySpark Migration: Prescribed Medicine Utilization for Hyperlipidemia, 2020.

Original: SAS/workshop_exercises/cond_pmed_2020.sas
          Stata/workshop_exercises/cond_mv_2020.do

Links MEPS-HC Medical Conditions file to Prescribed Medicines file for 2020:

National totals:
  - Total number of people with at least one PMED fill for hyperlipidemia (HL)
  - Total PMED fills for HL
  - Total PMED expenditures for HL

Per-person averages (among those with at least one PMED fill for HL):
  - Avg PMED fills for HL, by sex and poverty (POVCAT20)
  - Avg PMED expenditures for HL, by sex and poverty (POVCAT20)

Join chain (4-file):
  1. Conditions (h222) -> filter to CCSR=END010 (hyperlipidemia)
  2. CLNK (h220if1) -> inner join on CONDIDX
  3. CRITICAL: De-duplicate on EVNTIDX to avoid double-counting
  4. PMED (h220a) -> inner join on EVNTIDX
  5. Collapse to person-level
  6. Left join to FYC (h224), fill zeros

Input files:
  - h220a (2020 Prescribed Medicines file)
  - h222 (2020 Conditions file)
  - h220if1 (2020 CLNK: Condition-Event Link file)
  - h224 (2020 Full-Year Consolidated file)

Survey design: VARSTR (strata), VARPSU (cluster), PERWT20F (weight)
"""

from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, lit, sum as spark_sum, when


def run_etl(
    spark: SparkSession,
    pmed_path: str,
    cond_path: str,
    clnk_path: str,
    fyc_path: str,
    output_path: str,
) -> DataFrame:
    """Execute the cond_pmed_2020 ETL pipeline.

    Args:
        spark: Active SparkSession.
        pmed_path: Path to h220a (PMED file).
        cond_path: Path to h222 (Conditions file).
        clnk_path: Path to h220if1 (CLNK file).
        fyc_path: Path to h224 (FYC file).
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
        pmed_path: Path to h220a (PMED file).
        cond_path: Path to h222 (Conditions file).
        clnk_path: Path to h220if1 (CLNK file).
        fyc_path: Path to h224 (FYC file).

    Returns:
        Person-level DataFrame with HL PMED aggregates merged to FYC.
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

    Returns a dictionary of intermediate DataFrames at each checkpoint:
      - pmed: Raw PMED data
      - cond: Raw Conditions data
      - clnk: Raw CLNK data
      - fyc: FYC subset
      - hl_cond: Conditions filtered to hyperlipidemia (CCSR=END010)
      - cond_clnk: After CLNK join
      - cond_clnk_dedup: After EVNTIDX de-duplication (CRITICAL)
      - linked: After PMED join
      - person_level: Person-level aggregation
      - result: Final result after FYC left join

    Args:
        spark: Active SparkSession.
        pmed_path: Path to h220a.
        cond_path: Path to h222.
        clnk_path: Path to h220if1.
        fyc_path: Path to h224.

    Returns:
        Dictionary mapping checkpoint names to DataFrames.
    """
    intermediates = {}

    # Read input files
    pmed = spark.read.parquet(pmed_path)
    pmed = pmed.withColumnRenamed("LINKIDX", "EVNTIDX")
    intermediates["pmed"] = pmed

    cond = spark.read.parquet(cond_path)
    intermediates["cond"] = cond

    clnk = spark.read.parquet(clnk_path)
    intermediates["clnk"] = clnk

    fyc = spark.read.parquet(fyc_path).select(
        "DUPERSID", "AGELAST", "SEX", "CHOLDX", "POVCAT20",
        "VARSTR", "VARPSU", "PERWT20F"
    )
    intermediates["fyc"] = fyc

    # Step 1: Filter conditions to hyperlipidemia (any CCSR = END010)
    hl_cond = cond.filter(
        (col("CCSR1X") == "END010")
        | (col("CCSR2X") == "END010")
        | (col("CCSR3X") == "END010")
    )
    intermediates["hl_cond"] = hl_cond

    # Step 2: Join Conditions -> CLNK (inner join on DUPERSID + CONDIDX)
    cond_clnk = hl_cond.join(clnk, on=["DUPERSID", "CONDIDX"], how="inner")
    intermediates["cond_clnk"] = cond_clnk

    # Step 3: CRITICAL - De-duplicate on EVNTIDX
    # This matches SAS: proc sort nodupkey by dupersid evntidx
    # and Stata: duplicates drop evntidx, force
    # Prevents double-counting when multiple HL conditions link to the same event
    cond_clnk_dedup = cond_clnk.dropDuplicates(["DUPERSID", "EVNTIDX"])
    intermediates["cond_clnk_dedup"] = cond_clnk_dedup

    # Step 4: Join to PMED events (inner join on DUPERSID + EVNTIDX)
    linked = cond_clnk_dedup.join(pmed, on=["DUPERSID", "EVNTIDX"], how="inner")
    intermediates["linked"] = linked

    # Step 5: Collapse to person-level
    person_level = linked.groupBy("DUPERSID").agg(
        count("*").alias("n_hl_fills"),
        spark_sum("RXXP20X").alias("hl_drug_exp"),
    )
    intermediates["person_level"] = person_level

    # Step 6: Left join to FYC, fill zeros for non-matched persons
    result = fyc.join(person_level, on="DUPERSID", how="left")
    result = result.fillna({"n_hl_fills": 0, "hl_drug_exp": 0.0})
    result = result.withColumn(
        "hl_pmed_flag", when(col("n_hl_fills") > 0, 1).otherwise(0)
    )
    intermediates["result"] = result

    return intermediates
