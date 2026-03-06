"""PySpark Migration: Exercise 4d - Pooling FYC Files 2017-2019 with Variance Linkage.

Original: SAS/workshop_exercises/exercise_4d/Exercise4.sas

Pools 2017, 2018, and 2019 MEPS data and calculates:
  - Percentage of people with Joint Pain / Arthritis
  - Average expenditures per person, by Joint Pain status
  - Standard errors using common variance structure from Pooled Linkage file

Handles discontinuity from the 2018 MEPS CAPI redesign:
  - JTPAIN31 (2017) vs JTPAIN31_M18 (2018-2019)
  - 8-character DUPERSID (2017) vs 10-character (2018+)

Input files:
  - 2017 Full-year consolidated file (H201)
  - 2018 Full-year consolidated file (H209)
  - 2019 Full-year consolidated file (H216)
  - 1996-2019 Pooled linkage variance estimation file (H36U19)

Survey design: STRA9619 (strata), PSU9619 (cluster), PERWTF (pooled weight)
"""

from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, concat, length, lit, lpad, trim, when,
)


def run_etl(
    spark: SparkSession,
    fyc_2017_path: str,
    fyc_2018_path: str,
    fyc_2019_path: str,
    linkage_path: str,
    output_path: str,
) -> DataFrame:
    """Execute the Exercise 4d ETL pipeline.

    Args:
        spark: Active SparkSession.
        fyc_2017_path: Path to H201 (2017 FYC).
        fyc_2018_path: Path to H209 (2018 FYC).
        fyc_2019_path: Path to H216 (2019 FYC).
        linkage_path: Path to H36U19 (Pooled Linkage Variance file).
        output_path: Path to write output Parquet.

    Returns:
        Pooled DataFrame merged with variance linkage file.
    """
    result = build_etl_dataframe(
        spark, fyc_2017_path, fyc_2018_path, fyc_2019_path, linkage_path
    )
    result.write.mode("overwrite").parquet(output_path)
    return result


def build_etl_dataframe(
    spark: SparkSession,
    fyc_2017_path: str,
    fyc_2018_path: str,
    fyc_2019_path: str,
    linkage_path: str,
) -> DataFrame:
    """Build the pooled ETL DataFrame without writing output.

    Args:
        spark: Active SparkSession.
        fyc_2017_path: Path to H201.
        fyc_2018_path: Path to H209.
        fyc_2019_path: Path to H216.
        linkage_path: Path to H36U19.

    Returns:
        Pooled DataFrame merged with variance structure.
    """
    intermediates = build_etl_with_intermediates(
        spark, fyc_2017_path, fyc_2018_path, fyc_2019_path, linkage_path
    )
    return intermediates["result"]


def build_etl_with_intermediates(
    spark: SparkSession,
    fyc_2017_path: str,
    fyc_2018_path: str,
    fyc_2019_path: str,
    linkage_path: str,
) -> Dict[str, DataFrame]:
    """Build ETL with all intermediate DataFrames for testing.

    Handles the 2018 CAPI redesign discontinuity:
      - JTPAIN31 (2017) vs JTPAIN31_M18 (2018-2019)
      - 8-char DUPERSID (2017) -> 10-char via PANEL prefix
      - Year-specific weights and expenditure variables

    Returns:
        Dictionary of intermediate DataFrames.
    """
    intermediates = {}

    # Read 2017 FYC
    fyc17 = spark.read.parquet(fyc_2017_path).select(
        "DUPERSID", "PANEL", "VARSTR", "VARPSU", "PERWT17F",
        "AGELAST", "ARTHDX", "JTPAIN31", "TOTEXP17", "TOTSLF17"
    )
    fyc17 = (
        fyc17
        .withColumnRenamed("TOTEXP17", "TOTEXP")
        .withColumnRenamed("TOTSLF17", "TOTSLF")
        .withColumn("YEAR", lit(2017))
        .withColumn("PERWTF", col("PERWT17F") / 3)
    )
    # Handle JTPAIN31 for 2017 (same name, no suffix)
    fyc17 = fyc17.withColumn("JTPAIN_VAR", col("JTPAIN31"))
    # Convert 8-char DUPERSID to 10-char by prepending PANEL
    fyc17 = fyc17.withColumn(
        "DUPERSID",
        when(
            length(trim(col("DUPERSID"))) == 8,
            concat(lpad(col("PANEL").cast("string"), 2, "0"), col("DUPERSID"))
        ).otherwise(col("DUPERSID"))
    )
    intermediates["fyc17"] = fyc17

    # Read 2018 FYC
    fyc18 = spark.read.parquet(fyc_2018_path).select(
        "DUPERSID", "PANEL", "VARSTR", "VARPSU", "PERWT18F",
        "AGELAST", "ARTHDX", "JTPAIN31_M18", "TOTEXP18", "TOTSLF18"
    )
    fyc18 = (
        fyc18
        .withColumnRenamed("TOTEXP18", "TOTEXP")
        .withColumnRenamed("TOTSLF18", "TOTSLF")
        .withColumn("YEAR", lit(2018))
        .withColumn("PERWTF", col("PERWT18F") / 3)
        .withColumnRenamed("JTPAIN31_M18", "JTPAIN_VAR")
    )
    intermediates["fyc18"] = fyc18

    # Read 2019 FYC
    fyc19 = spark.read.parquet(fyc_2019_path).select(
        "DUPERSID", "PANEL", "VARSTR", "VARPSU", "PERWT19F",
        "AGELAST", "ARTHDX", "JTPAIN31_M18", "TOTEXP19", "TOTSLF19"
    )
    fyc19 = (
        fyc19
        .withColumnRenamed("TOTEXP19", "TOTEXP")
        .withColumnRenamed("TOTSLF19", "TOTSLF")
        .withColumn("YEAR", lit(2019))
        .withColumn("PERWTF", col("PERWT19F") / 3)
        .withColumnRenamed("JTPAIN31_M18", "JTPAIN_VAR")
    )
    intermediates["fyc19"] = fyc19

    # Select common columns for union
    common_cols = [
        "DUPERSID", "PANEL", "VARSTR", "VARPSU", "AGELAST",
        "ARTHDX", "JTPAIN_VAR", "TOTEXP", "TOTSLF", "YEAR", "PERWTF"
    ]

    pool = (
        fyc17.select(common_cols)
        .unionByName(fyc18.select(common_cols))
        .unionByName(fyc19.select(common_cols))
    )

    # Create JOINT_PAIN variable and SPOP (subpopulation: age 18+)
    # NOTE: ARTHDX threshold differs by year due to 2018 CAPI redesign:
    #   2017: exclude when ARTHDX <= 0 AND JTPAIN31 < 0
    #   2018-2019: exclude when ARTHDX < 0 AND JTPAIN31_M18 < 0
    pool = pool.withColumn(
        "SPOP",
        when(
            (col("AGELAST") >= 18)
            & ~(
                when(col("YEAR") == 2017, col("ARTHDX") <= 0)
                .otherwise(col("ARTHDX") < 0)
                & (col("JTPAIN_VAR") < 0)
            ),
            1
        ).otherwise(0)
    )

    pool = pool.withColumn(
        "JOINT_PAIN",
        when(
            (col("SPOP") == 1)
            & ((col("ARTHDX") == 1) | (col("JTPAIN_VAR") == 1)),
            1
        ).when(col("SPOP") == 1, 2)
    )

    # Flag zero weights for QC
    pool = pool.withColumn(
        "ZERO_WEIGHT", when(col("PERWTF") == 0, 1).otherwise(0)
    )
    intermediates["pool"] = pool

    # Read Pooled Linkage Variance file
    vs_file = spark.read.parquet(linkage_path)

    # Convert 8-char DUPERSID to 10-char for pre-2018 panels
    vs_file = vs_file.withColumn(
        "DUPERSID",
        when(
            length(trim(col("DUPERSID"))) == 8,
            concat(
                lpad(col("PANEL").cast("string"), 2, "0"),
                col("DUPERSID")
            )
        ).otherwise(col("DUPERSID"))
    )

    # Filter to panels 21-24 (2017-2019)
    vs_file = vs_file.filter(col("PANEL").isin([21, 22, 23, 24]))
    vs_file = vs_file.dropDuplicates(["DUPERSID"])
    intermediates["vs_file"] = vs_file

    # Select only the variance structure columns from linkage file
    vs_cols = vs_file.select("DUPERSID", "STRA9619", "PSU9619")

    # Merge pooled data with variance structure (left join preserves all pooled records)
    result = pool.join(vs_cols, on="DUPERSID", how="left")
    intermediates["result"] = result

    return intermediates
