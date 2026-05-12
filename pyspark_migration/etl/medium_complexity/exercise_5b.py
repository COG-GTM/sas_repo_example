"""
PySpark migration of Exercise5b.sas

DESCRIPTION: Construct insurance status variables from monthly insurance variables
  - Count # of months with each type of insurance
  - Create flags for full-year insured, group insurance, non-group insurance

Original: SAS/workshop_exercises/exercise_5b/Exercise5b.sas
Input: H181.SAS7BDAT (2015 FY PUF)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT15F (weight)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit
from functools import reduce


# Monthly insurance variable prefixes and month codes
MONTHS = ["JA", "FE", "MA", "AP", "MY", "JU", "JL", "AU", "SE", "OC", "NO", "DE"]
YR = "15"


def _count_months(df: DataFrame, prefix: str, yr: str, condition_val: int = 1) -> DataFrame:
    """Count months where a given insurance variable equals condition_val."""
    month_cols = [f"{prefix}{m}{yr}" for m in MONTHS]
    # Handle X-suffix variables
    existing = [c for c in month_cols if c in df.columns]
    if not existing:
        # Try with X suffix
        month_cols = [f"{prefix}{m}{yr}X" for m in MONTHS]
        existing = [c for c in month_cols if c in df.columns]

    if not existing:
        return df.withColumn(f"{prefix}_N", lit(0))

    count_expr = reduce(
        lambda a, b: a + b,
        [when(col(c) == condition_val, 1).otherwise(0) for c in existing]
    )
    return df.withColumn(f"{prefix}_N", count_expr)


def run_etl(spark: SparkSession, input_df: DataFrame) -> DataFrame:
    """
    Transform FYC 2015 data to count monthly insurance coverage.

    Replicates the SAS DATA step that:
    - Counts months of coverage for each insurance type
    - Creates flags: FULL_INSU, GROUP_INS1, GROUP_INS2, NG_INS
    """
    result = input_df

    # Count months for each insurance type
    result = _count_months(result, "PRI", YR)

    # For INS (insured), count months where INS**X = 1
    ins_cols = [f"INS{m}{YR}X" for m in MONTHS]
    existing_ins = [c for c in ins_cols if c in result.columns]
    if existing_ins:
        ins_count = reduce(
            lambda a, b: a + b,
            [when(col(c) == 1, 1).otherwise(0) for c in existing_ins]
        )
        result = result.withColumn("INS_N", ins_count)

        # UNINS: months where INS = 2
        unins_count = reduce(
            lambda a, b: a + b,
            [when(col(c) == 2, 1).otherwise(0) for c in existing_ins]
        )
        result = result.withColumn("UNINS_N", unins_count)

        # REF: months in survey (INS > 0)
        ref_count = reduce(
            lambda a, b: a + b,
            [when(col(c) > 0, 1).otherwise(0) for c in existing_ins]
        )
        result = result.withColumn("REF_N", ref_count)
    else:
        result = (
            result
            .withColumn("INS_N", lit(0))
            .withColumn("UNINS_N", lit(0))
            .withColumn("REF_N", lit(0))
        )

    result = _count_months(result, "MCD", YR)
    result = _count_months(result, "MCR", YR)
    result = _count_months(result, "TRI", YR)

    # OPA/OPB combined count
    opa_cols = [f"OPA{m}{YR}" for m in MONTHS]
    opb_cols = [f"OPB{m}{YR}" for m in MONTHS]
    existing_opa = [c for c in opa_cols if c in result.columns]
    existing_opb = [c for c in opb_cols if c in result.columns]
    if existing_opa and existing_opb:
        opab_count = reduce(
            lambda a, b: a + b,
            [when((col(a) == 1) | (col(b) == 1), 1).otherwise(0)
             for a, b in zip(existing_opa, existing_opb)]
        )
        result = result.withColumn("OPAB_N", opab_count)
    else:
        result = result.withColumn("OPAB_N", lit(0))

    # GRP: PEG or TRI or POU or PDK = 1
    peg_cols = [f"PEG{m}{YR}" for m in MONTHS]
    tri_cols = [f"TRI{m}{YR}X" for m in MONTHS]
    pou_cols = [f"POU{m}{YR}" for m in MONTHS]
    pdk_cols = [f"PDK{m}{YR}" for m in MONTHS]
    all_grp = list(zip(
        [c for c in peg_cols if c in result.columns],
        [c for c in tri_cols if c in result.columns],
        [c for c in pou_cols if c in result.columns],
        [c for c in pdk_cols if c in result.columns]
    ))
    if all_grp:
        grp_count = reduce(
            lambda a, b: a + b,
            [when((col(p) == 1) | (col(t) == 1) | (col(o) == 1) | (col(d) == 1), 1).otherwise(0)
             for p, t, o, d in all_grp]
        )
        result = result.withColumn("GRP_N", grp_count)
    else:
        result = result.withColumn("GRP_N", lit(0))

    # NG: PRX or PNG or POG or PRS = 1
    prx_cols = [f"PRX{m}{YR}" for m in MONTHS]
    png_cols = [f"PNG{m}{YR}" for m in MONTHS]
    pog_cols = [f"POG{m}{YR}" for m in MONTHS]
    prs_cols = [f"PRS{m}{YR}" for m in MONTHS]
    all_ng = list(zip(
        [c for c in prx_cols if c in result.columns],
        [c for c in png_cols if c in result.columns],
        [c for c in pog_cols if c in result.columns],
        [c for c in prs_cols if c in result.columns]
    ))
    if all_ng:
        ng_count = reduce(
            lambda a, b: a + b,
            [when((col(p) == 1) | (col(n) == 1) | (col(o) == 1) | (col(s) == 1), 1).otherwise(0)
             for p, n, o, s in all_ng]
        )
        result = result.withColumn("NG_N", ng_count)
    else:
        result = result.withColumn("NG_N", lit(0))

    # PUB: MCR or MCD or OPA or OPB = 1
    mcr_cols = [f"MCR{m}{YR}X" for m in MONTHS]
    mcd_cols = [f"MCD{m}{YR}X" for m in MONTHS]
    all_pub = list(zip(
        [c for c in mcr_cols if c in result.columns],
        [c for c in mcd_cols if c in result.columns],
        [c for c in opa_cols if c in result.columns],
        [c for c in opb_cols if c in result.columns]
    ))
    if all_pub:
        pub_count = reduce(
            lambda a, b: a + b,
            [when((col(m) == 1) | (col(d) == 1) | (col(a) == 1) | (col(b) == 1), 1).otherwise(0)
             for m, d, a, b in all_pub]
        )
        result = result.withColumn("PUB_N", pub_count)
    else:
        result = result.withColumn("PUB_N", lit(0))

    # Create insurance flags
    result = (
        result
        .withColumn("FULL_INSU", when(col("UNINS_N") == 0, 1).otherwise(0))
        .withColumn("GROUP_INS1", when(col("GRP_N") > 0, 1).otherwise(0))
        .withColumn("GROUP_INS2",
                     when((col("GRP_N") > 0) & (col("GRP_N") == col("REF_N")), 1).otherwise(0))
        .withColumn("NG_INS", when(col("NG_N") > 0, 1).otherwise(0))
    )

    return result
