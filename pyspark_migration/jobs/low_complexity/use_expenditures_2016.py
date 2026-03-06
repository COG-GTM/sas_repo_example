"""PySpark Migration: Use Expenditures 2016 - Expenditures by Event Type and SOP.

Original: SAS/summary_tables_examples/use_expenditures_2016.sas
          R/summary_tables_examples/use_expenditures_2016.R

Replicates estimates from MEPS-HC Data Tools summary tables:
  - Total expenditures by event type and source of payment (SOP)
  - Mean expenditure per person
  - Mean out-of-pocket (SLF) payment per person with an out-of-pocket expense

Event types: OBV (Office-based visits), OBD (Office-based physician),
             OPT (Outpatient visits), OPV/OPS (Outpatient physician)

Input: H192 (2016 Full-Year Consolidated file)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT16F (weight)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when


def run_etl(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """Execute the Use Expenditures 2016 ETL pipeline.

    Args:
        spark: Active SparkSession.
        input_path: Path to H192 data file.
        output_path: Path to write output Parquet.

    Returns:
        Transformed DataFrame.
    """
    df = build_etl_dataframe(spark, input_path)
    df.write.mode("overwrite").parquet(output_path)
    return df


def build_etl_dataframe(spark: SparkSession, input_path: str) -> DataFrame:
    """Build the ETL DataFrame without writing output.

    Args:
        spark: Active SparkSession.
        input_path: Path to H192 data file.

    Returns:
        Transformed DataFrame.
    """
    h192 = spark.read.parquet(input_path)

    # Aggregate payment sources
    # PTR = Private (PRV) + TRICARE (TRI)
    # OTZ = OFD + STL + OPR + OPU + OSR + WCP + VA

    # Office-based visits
    h192 = h192.withColumn("OBVPTR", col("OBVPRV16") + col("OBVTRI16"))
    h192 = h192.withColumn(
        "OBVOTZ",
        col("OBVOFD16") + col("OBVSTL16") + col("OBVOPR16")
        + col("OBVOPU16") + col("OBVOSR16") + col("OBVWCP16") + col("OBVVA16")
    )

    # Office-based physician visits
    h192 = h192.withColumn("OBDPTR", col("OBDPRV16") + col("OBDTRI16"))
    h192 = h192.withColumn(
        "OBDOTZ",
        col("OBDOFD16") + col("OBDSTL16") + col("OBDOPR16")
        + col("OBDOPU16") + col("OBDOSR16") + col("OBDWCP16") + col("OBDVA16")
    )

    # Outpatient visits (facility + SBD)
    h192 = h192.withColumn("OPTPTR", col("OPTPRV16") + col("OPTTRI16"))
    h192 = h192.withColumn(
        "OPTOTZ",
        col("OPTOFD16") + col("OPTSTL16") + col("OPTOPR16")
        + col("OPTOPU16") + col("OPTOSR16") + col("OPTWCP16") + col("OPTVA16")
    )

    # Outpatient physician visits (facility expense)
    h192 = h192.withColumn("OPVPTR", col("OPVPRV16") + col("OPVTRI16"))
    h192 = h192.withColumn(
        "OPVOTZ",
        col("OPVOFD16") + col("OPVSTL16") + col("OPVOPR16")
        + col("OPVOPU16") + col("OPVOSR16") + col("OPVWCP16") + col("OPVVA16")
    )

    # Outpatient physician visits (SBD expense)
    h192 = h192.withColumn("OPSPTR", col("OPSPRV16") + col("OPSTRI16"))
    h192 = h192.withColumn(
        "OPSOTZ",
        col("OPSOFD16") + col("OPSSTL16") + col("OPSOPR16")
        + col("OPSOPU16") + col("OPSOSR16") + col("OPSWCP16") + col("OPSVA16")
    )

    # Combine facility and SBD for outpatient physician visits
    h192 = h192.withColumn("OPpSLF", col("OPVSLF16") + col("OPSSLF16"))
    h192 = h192.withColumn("OPpMCR", col("OPVMCR16") + col("OPSMCR16"))
    h192 = h192.withColumn("OPpMCD", col("OPVMCD16") + col("OPSMCD16"))
    h192 = h192.withColumn("OPpPTR", col("OPVPTR") + col("OPSPTR"))
    h192 = h192.withColumn("OPpOTZ", col("OPVOTZ") + col("OPSOTZ"))

    # Define domains for persons with out-of-pocket expense
    h192 = h192.withColumn("has_OBVSLF", (col("OBVSLF16") > 0).cast("int"))
    h192 = h192.withColumn("has_OBDSLF", (col("OBDSLF16") > 0).cast("int"))
    h192 = h192.withColumn("has_OPTSLF", (col("OPTSLF16") > 0).cast("int"))
    h192 = h192.withColumn("has_OPpSLF", (col("OPpSLF") > 0).cast("int"))

    return h192
