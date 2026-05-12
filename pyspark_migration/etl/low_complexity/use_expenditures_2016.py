"""
PySpark migration of use_expenditures_2016.sas

DESCRIPTION: Use, expenditures, and population, 2016
  - Expenditures by event type and source of payment (SOP)
  - Total expenditures, mean expenditure per person
  - Mean out-of-pocket payment per person with an out-of-pocket expense

Original: SAS/summary_tables_examples/use_expenditures_2016.sas
Input: h192.ssp (2016 Full-Year Consolidated)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT16F (weight)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when


def run_etl(spark: SparkSession, input_df: DataFrame) -> DataFrame:
    """
    Transform FYC 2016 data for expenditure-by-SOP analysis.

    Replicates the SAS DATA step that:
    - Aggregates payment sources into PTR (private+TRICARE) and OTZ (other)
    - Combines facility and SBD expenses for outpatient physician visits
    - Creates domain flags for persons with out-of-pocket expense
    """
    # Select needed columns (these are the main expenditure variables)
    result = input_df

    # Office-based visits: PTR and OTZ aggregates
    result = (
        result
        .withColumn("OBVPTR", col("OBVPRV16") + col("OBVTRI16"))
        .withColumn("OBVOTZ",
                     col("OBVOFD16") + col("OBVSTL16") + col("OBVOPR16") +
                     col("OBVOPU16") + col("OBVOSR16") + col("OBVWCP16") + col("OBVVA16"))
    )

    # Office-based physician visits
    result = (
        result
        .withColumn("OBDPTR", col("OBDPRV16") + col("OBDTRI16"))
        .withColumn("OBDOTZ",
                     col("OBDOFD16") + col("OBDSTL16") + col("OBDOPR16") +
                     col("OBDOPU16") + col("OBDOSR16") + col("OBDWCP16") + col("OBDVA16"))
    )

    # Outpatient visits (facility + SBD)
    result = (
        result
        .withColumn("OPTPTR", col("OPTPRV16") + col("OPTTRI16"))
        .withColumn("OPTOTZ",
                     col("OPTOFD16") + col("OPTSTL16") + col("OPTOPR16") +
                     col("OPTOPU16") + col("OPTOSR16") + col("OPTWCP16") + col("OPTVA16"))
    )

    # Outpatient physician visits (facility expense)
    result = (
        result
        .withColumn("OPVPTR", col("OPVPRV16") + col("OPVTRI16"))
        .withColumn("OPVOTZ",
                     col("OPVOFD16") + col("OPVSTL16") + col("OPVOPR16") +
                     col("OPVOPU16") + col("OPVOSR16") + col("OPVWCP16") + col("OPVVA16"))
    )

    # Outpatient physician visits (SBD expense)
    result = (
        result
        .withColumn("OPSPTR", col("OPSPRV16") + col("OPSTRI16"))
        .withColumn("OPSOTZ",
                     col("OPSOFD16") + col("OPSSTL16") + col("OPSOPR16") +
                     col("OPSOPU16") + col("OPSOSR16") + col("OPSWCP16") + col("OPSVA16"))
    )

    # Combine facility and SBD for outpatient physician visits
    result = (
        result
        .withColumn("OPpSLF", col("OPVSLF16") + col("OPSSLF16"))
        .withColumn("OPpMCR", col("OPVMCR16") + col("OPSMCR16"))
        .withColumn("OPpMCD", col("OPVMCD16") + col("OPSMCD16"))
        .withColumn("OPpPTR", col("OPVPTR") + col("OPSPTR"))
        .withColumn("OPpOTZ", col("OPVOTZ") + col("OPSOTZ"))
    )

    # Domain flags for persons with out-of-pocket expense
    result = (
        result
        .withColumn("has_OBVSLF", (col("OBVSLF16") > 0).cast("int"))
        .withColumn("has_OBDSLF", (col("OBDSLF16") > 0).cast("int"))
        .withColumn("has_OPTSLF", (col("OPTSLF16") > 0).cast("int"))
        .withColumn("has_OPpSLF", (col("OPpSLF") > 0).cast("int"))
    )

    return result
