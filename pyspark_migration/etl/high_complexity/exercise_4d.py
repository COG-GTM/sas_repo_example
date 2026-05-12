"""
PySpark migration of Exercise4.sas (exercise_4d)

DESCRIPTION: Pool MEPS data files from 2017, 2018, and 2019
  - Percentage of people with Joint Pain / Arthritis
  - Average expenditures per person, by Joint Pain status
  - Uses Pooled Linkage Variance Estimation file (HC-036) for correct SEs

Original: SAS/workshop_exercises/exercise_4d/Exercise4.sas
Inputs:
  - H201 (2017 FYC), H209 (2018 FYC), H216 (2019 FYC)
  - H36U19 (1996-2019 Pooled Linkage Variance file)

Survey design: stra9619 (strata), psu9619 (cluster), perwtf (pooled weight)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, concat, lpad


def run_etl(
    spark: SparkSession,
    fyc_2017: DataFrame,
    fyc_2018: DataFrame,
    fyc_2019: DataFrame,
    variance_file: DataFrame = None
) -> DataFrame:
    """
    Pool 2017-2019 FYC files and merge with pooled linkage variance file.

    Replicates the SAS DATA step that:
    - Selects and renames year-specific variables
    - Stacks three years
    - Creates pooled weight (perwtf = year_weight / 3)
    - Creates JOINT_PAIN variable (from ARTHDX, JTPAIN31/JTPAIN31_M18)
    - Creates SPOP (subpopulation: age 18+)
    - Converts 8-char DUPERSID to 10-char for 2017 data
    - Merges with pooled linkage variance file for correct SEs
    """
    # 2017: select and rename
    yr17 = (
        fyc_2017
        .select("DUPERSID", "PANEL", "VARSTR", "VARPSU", "PERWT17F",
                "AGELAST", "ARTHDX", "JTPAIN31", "TOTEXP17", "TOTSLF17")
        .withColumn("year", lit(2017))
        .withColumnRenamed("TOTEXP17", "totexp")
        .withColumnRenamed("TOTSLF17", "totslf")
        .withColumn("perwtf", col("PERWT17F") / 3)
    )

    # Fix DUPERSID for 2017 (8-char to 10-char by prepending panel)
    yr17 = yr17.withColumn(
        "DUPERSID",
        concat(lpad(col("PANEL").cast("string"), 2, "0"), col("DUPERSID"))
    )

    # Create joint_pain for 2017
    yr17 = yr17.withColumn(
        "spop",
        when(
            (col("AGELAST") >= 18) &
            ~((col("ARTHDX") <= 0) & (col("JTPAIN31") < 0)),
            1
        ).otherwise(0)
    ).withColumn(
        "joint_pain",
        when(
            (col("spop") == 1) & ((col("ARTHDX") == 1) | (col("JTPAIN31") == 1)),
            1
        ).when(col("spop") == 1, 2)
    )

    # 2018: select and rename
    yr18 = (
        fyc_2018
        .select("DUPERSID", "PANEL", "VARSTR", "VARPSU", "PERWT18F",
                "AGELAST", "ARTHDX", "JTPAIN31_M18", "TOTEXP18", "TOTSLF18")
        .withColumn("year", lit(2018))
        .withColumnRenamed("TOTEXP18", "totexp")
        .withColumnRenamed("TOTSLF18", "totslf")
        .withColumn("perwtf", col("PERWT18F") / 3)
    )

    yr18 = yr18.withColumn(
        "spop",
        when(
            (col("AGELAST") >= 18) &
            ~((col("ARTHDX") < 0) & (col("JTPAIN31_M18") < 0)),
            1
        ).otherwise(0)
    ).withColumn(
        "joint_pain",
        when(
            (col("spop") == 1) & ((col("ARTHDX") == 1) | (col("JTPAIN31_M18") == 1)),
            1
        ).when(col("spop") == 1, 2)
    )

    # 2019: select and rename
    yr19 = (
        fyc_2019
        .select("DUPERSID", "PANEL", "VARSTR", "VARPSU", "PERWT19F",
                "AGELAST", "ARTHDX", "JTPAIN31_M18", "TOTEXP19", "TOTSLF19")
        .withColumn("year", lit(2019))
        .withColumnRenamed("TOTEXP19", "totexp")
        .withColumnRenamed("TOTSLF19", "totslf")
        .withColumn("perwtf", col("PERWT19F") / 3)
    )

    yr19 = yr19.withColumn(
        "spop",
        when(
            (col("AGELAST") >= 18) &
            ~((col("ARTHDX") < 0) & (col("JTPAIN31_M18") < 0)),
            1
        ).otherwise(0)
    ).withColumn(
        "joint_pain",
        when(
            (col("spop") == 1) & ((col("ARTHDX") == 1) | (col("JTPAIN31_M18") == 1)),
            1
        ).when(col("spop") == 1, 2)
    )

    # Select common columns and union
    common_cols = [
        "DUPERSID", "PANEL", "VARSTR", "VARPSU", "AGELAST",
        "ARTHDX", "year", "totexp", "totslf", "perwtf", "spop", "joint_pain"
    ]
    pool = yr17.select(*common_cols).union(yr18.select(*common_cols)).union(yr19.select(*common_cols))

    # Zero-weight flag
    pool = pool.withColumn(
        "zero_weight",
        when(col("perwtf") == 0, 1).otherwise(0)
    )

    # Merge with pooled linkage variance file if provided
    if variance_file is not None:
        # Fix DUPERSID in variance file for pre-2018 records
        vs = variance_file.withColumn(
            "DUPERSID",
            when(
                col("DUPERSID").rlike("^[0-9]{8}$"),
                concat(lpad(col("PANEL").cast("string"), 2, "0"), col("DUPERSID"))
            ).otherwise(col("DUPERSID"))
        )

        # Filter to panels 21-24 and deduplicate
        vs = vs.filter(col("PANEL").isin(21, 22, 23, 24)).dropDuplicates(["DUPERSID"])

        # Left join to pool
        pool = pool.join(
            vs.select("DUPERSID", "stra9619", "psu9619"),
            on="DUPERSID",
            how="left"
        )

    return pool
