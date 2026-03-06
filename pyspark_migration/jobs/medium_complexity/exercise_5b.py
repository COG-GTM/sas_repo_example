"""PySpark Migration: Exercise 5b - Insurance Status from Monthly Variables, 2015.

Original: SAS/workshop_exercises/exercise_5b/Exercise5b.sas

Illustrates how to construct insurance status variables from monthly insurance
variables in the person-level data:
  - Count # of months with each insurance type
  - Create flags for various insurance statuses:
    - FULL_INSU: Insured for full year (UNINS_N = 0)
    - GROUP_INS1: Ever insured by private group
    - GROUP_INS2: Insured by private group for full year
    - NG_INS: Ever insured by private non-group

Then estimates % of persons covered by each insurance type, by race/ethnicity.

Input: H181 (2015 Full-Year Consolidated file)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT15F (weight)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when


# Month abbreviations used in MEPS variable names
MONTHS = ["JA", "FE", "MA", "AP", "MY", "JU", "JL", "AU", "SE", "OC", "NO", "DE"]
YR = "15"


def run_etl(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """Execute the Exercise 5b ETL pipeline.

    Args:
        spark: Active SparkSession.
        input_path: Path to H181 data file.
        output_path: Path to write output Parquet.

    Returns:
        Transformed DataFrame with insurance count and flag variables.
    """
    df = build_etl_dataframe(spark, input_path)
    df.write.mode("overwrite").parquet(output_path)
    return df


def build_etl_dataframe(spark: SparkSession, input_path: str) -> DataFrame:
    """Build the ETL DataFrame without writing output.

    Args:
        spark: Active SparkSession.
        input_path: Path to H181 data file.

    Returns:
        Transformed DataFrame.
    """
    h181 = spark.read.parquet(input_path)

    # Count months with each insurance type
    # PRI: Private insurance
    pri_cols = [f"PRI{m}{YR}" for m in MONTHS]
    h181 = h181.withColumn(
        "PRI_N",
        sum(when(col(c) == 1, 1).otherwise(0) for c in pri_cols)
    )

    # INS: Any insurance (edited variable INSmmYYX)
    ins_cols = [f"INS{m}{YR}X" for m in MONTHS]
    h181 = h181.withColumn(
        "INS_N",
        sum(when(col(c) == 1, 1).otherwise(0) for c in ins_cols)
    )

    # UNINS: Uninsured months (INS == 2)
    h181 = h181.withColumn(
        "UNINS_N",
        sum(when(col(c) == 2, 1).otherwise(0) for c in ins_cols)
    )

    # MCD: Medicaid (edited variable MCDmmYYX)
    mcd_cols = [f"MCD{m}{YR}X" for m in MONTHS]
    h181 = h181.withColumn(
        "MCD_N",
        sum(when(col(c) == 1, 1).otherwise(0) for c in mcd_cols)
    )

    # MCR: Medicare (edited variable MCRmmYYX)
    mcr_cols = [f"MCR{m}{YR}X" for m in MONTHS]
    h181 = h181.withColumn(
        "MCR_N",
        sum(when(col(c) == 1, 1).otherwise(0) for c in mcr_cols)
    )

    # TRI: TRICARE (edited variable TRImmYYX)
    tri_cols = [f"TRI{m}{YR}X" for m in MONTHS]
    h181 = h181.withColumn(
        "TRI_N",
        sum(when(col(c) == 1, 1).otherwise(0) for c in tri_cols)
    )

    # OPA/OPB: Other public A or B
    opa_cols = [f"OPA{m}{YR}" for m in MONTHS]
    opb_cols = [f"OPB{m}{YR}" for m in MONTHS]
    h181 = h181.withColumn(
        "OPAB_N",
        sum(
            when((col(opa_cols[i]) == 1) | (col(opb_cols[i]) == 1), 1).otherwise(0)
            for i in range(12)
        )
    )

    # GRP: Group insurance (PEG or TRI or POU or PDK)
    peg_cols = [f"PEG{m}{YR}" for m in MONTHS]
    pou_cols = [f"POU{m}{YR}" for m in MONTHS]
    pdk_cols = [f"PDK{m}{YR}" for m in MONTHS]
    h181 = h181.withColumn(
        "GRP_N",
        sum(
            when(
                (col(peg_cols[i]) == 1) | (col(tri_cols[i]) == 1)
                | (col(pou_cols[i]) == 1) | (col(pdk_cols[i]) == 1),
                1
            ).otherwise(0)
            for i in range(12)
        )
    )

    # NG: Non-group insurance (PRX or PNG or POG or PRS)
    prx_cols = [f"PRX{m}{YR}" for m in MONTHS]
    png_cols = [f"PNG{m}{YR}" for m in MONTHS]
    pog_cols = [f"POG{m}{YR}" for m in MONTHS]
    prs_cols = [f"PRS{m}{YR}" for m in MONTHS]
    h181 = h181.withColumn(
        "NG_N",
        sum(
            when(
                (col(prx_cols[i]) == 1) | (col(png_cols[i]) == 1)
                | (col(pog_cols[i]) == 1) | (col(prs_cols[i]) == 1),
                1
            ).otherwise(0)
            for i in range(12)
        )
    )

    # PUB: Public insurance (MCR or MCD or OPA or OPB)
    h181 = h181.withColumn(
        "PUB_N",
        sum(
            when(
                (col(mcr_cols[i]) == 1) | (col(mcd_cols[i]) == 1)
                | (col(opa_cols[i]) == 1) | (col(opb_cols[i]) == 1),
                1
            ).otherwise(0)
            for i in range(12)
        )
    )

    # REF: Months in survey (INS > 0)
    h181 = h181.withColumn(
        "REF_N",
        sum(when(col(c) > 0, 1).otherwise(0) for c in ins_cols)
    )

    # Create insurance flags
    h181 = h181.withColumn(
        "FULL_INSU", when(col("UNINS_N") == 0, 1).otherwise(0)
    )
    h181 = h181.withColumn(
        "GROUP_INS1", when(col("GRP_N") > 0, 1).otherwise(0)
    )
    h181 = h181.withColumn(
        "GROUP_INS2",
        when((col("GRP_N") > 0) & (col("GRP_N") == col("REF_N")), 1).otherwise(0)
    )
    h181 = h181.withColumn(
        "NG_INS", when(col("NG_N") > 0, 1).otherwise(0)
    )

    return h181
