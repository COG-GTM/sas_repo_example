"""
PySpark migration of cond_mv_2020.do (Stata)

DESCRIPTION: Office-based visits (or Prescribed medicines) for mental health /
  hyperlipidemia treatment, 2020

  This Stata script performs the same 4-file join chain as cond_pmed_2020.sas
  but from the Stata perspective. The ETL logic is identical:
  - Filter conditions to hyperlipidemia (CCSR = END010)
  - Join to CLNK by CONDIDX
  - De-duplicate on EVNTIDX
  - Join to PMED by EVNTIDX
  - Collapse to person level
  - Left join to FYC

Original: Stata/workshop_exercises/cond_mv_2020.do
Inputs:
  - h220a.dta  (2020 Prescribed Medicines)
  - h222.dta   (2020 Conditions)
  - h220if1.dta (2020 CLNK)
  - h224.dta   (2020 FYC)

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
    Execute the Stata-equivalent 4-file join chain.

    Returns intermediate DataFrames at each checkpoint.
    Note: Column names are lowercased to match Stata convention (rename *, lower).
    The logic is otherwise identical to the SAS version.
    """
    intermediates = {}

    # Stata renames all vars to lowercase
    pmed = pmed_df.select(
        col("DUPERSID").alias("dupersid"),
        col("DRUGIDX").alias("drugidx"),
        col("RXRECIDX").alias("rxrecidx"),
        col("LINKIDX").alias("evntidx"),
        col("RXDRGNAM").alias("rxdrgnam"),
        col("RXXP20X").alias("rxxp20x"),
    )

    cond = cond_df.select(
        col("DUPERSID").alias("dupersid"),
        col("CONDIDX").alias("condidx"),
        col("ICD10CDX").alias("icd10cdx"),
        col("CCSR1X").alias("ccsr1x"),
        col("CCSR2X").alias("ccsr2x"),
        col("CCSR3X").alias("ccsr3x"),
    )

    clnk = clnk_df.select(
        col("DUPERSID").alias("dupersid"),
        col("CONDIDX").alias("condidx"),
        col("EVNTIDX").alias("evntidx"),
        col("EVENTYPE").alias("eventype"),
    )

    fyc = fyc_df.select(
        col("DUPERSID").alias("dupersid"),
        col("SEX").alias("sex"),
        col("AGELAST").alias("agelast"),
        col("CHOLDX").alias("choldx"),
        col("POVCAT20").alias("povcat20"),
        col("VARSTR").alias("varstr"),
        col("VARPSU").alias("varpsu"),
        col("PERWT20F").alias("perwt20f"),
    )

    # Step 1: Filter conditions to hyperlipidemia
    hl_cond = cond.filter(
        (col("ccsr1x") == "END010") |
        (col("ccsr2x") == "END010") |
        (col("ccsr3x") == "END010")
    )
    intermediates["hl_cond"] = hl_cond

    # Step 2: Merge conditions to CLNK by condidx (m:m in Stata, inner join)
    cond_clnk = hl_cond.join(clnk, on=["dupersid", "condidx"], how="inner")
    intermediates["cond_clnk"] = cond_clnk

    # Step 3: De-duplicate on evntidx (Stata: duplicates drop evntidx, force)
    cond_clnk_dedup = cond_clnk.dropDuplicates(["evntidx"])
    intermediates["cond_clnk_dedup"] = cond_clnk_dedup

    # Step 4: Merge to PMED by evntidx (1:m, inner)
    hl_merged = cond_clnk_dedup.join(pmed, on=["dupersid", "evntidx"], how="inner")
    intermediates["hl_merged"] = hl_merged

    # Step 5: Collapse to person level
    person_level = (
        hl_merged
        .groupBy("dupersid")
        .agg(
            count("*").alias("num_rx"),
            spark_sum("rxxp20x").alias("exp_rx")
        )
    )
    intermediates["person_level"] = person_level

    # Step 6: Merge to FYC, fill zeros
    # Stata: replace exp_rx=0 if _merge==2, replace num_rx=0 if _merge==2
    fyc_with_hl_ever = fyc.withColumn(
        "HL_ever",
        when(col("choldx") == 1, 1)
        .when(col("choldx") == 2, 0)
    )

    result = (
        fyc_with_hl_ever
        .join(person_level, on="dupersid", how="left")
        .fillna({"num_rx": 0, "exp_rx": 0.0})
        .withColumn("any_rx", (col("num_rx") > 0).cast("int"))
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
